import gleam/base
import gleam/bit_string
import gleam/dynamic.{Dynamic}
import gleam/option.{None, Option, Some}
import gleam/list
import gleam/map.{Map}
import gleam/result
import gleam/json
import glats.{Connection}
import glats/message.{Message}
import glats/jetstream.{JetstreamError}
import glats/internal/js

const stream_prefix = "$JS.API.STREAM"

/// Info about stream returned from `info` function.
///
pub type StreamInfo {
  StreamInfo(created: String, config: StreamConfig, state: StreamState)
}

/// Available config options for a single stream.
///
pub type StreamConfig {
  StreamConfig(
    name: String,
    subjects: List(String),
    retention: Option(String),
    max_consumers: Option(Int),
    max_msgs: Option(Int),
    max_bytes: Option(Int),
    max_age: Option(Int),
    max_msgs_per_subject: Option(Int),
    max_msg_size: Option(Int),
    discard: Option(String),
    storage: Option(String),
    num_replicas: Option(Int),
    duplicate_window: Option(Int),
    allow_direct: Option(Bool),
    mirror_direct: Option(Bool),
    sealed: Option(Bool),
    deny_delete: Option(Bool),
    deny_purge: Option(Bool),
    allow_rollup_hdrs: Option(Bool),
  )
}

/// Stream state.
///
pub type StreamState {
  StreamState(
    messages: Int,
    bytes: Int,
    first_seq: Int,
    first_ts: String,
    last_seq: Int,
    last_ts: String,
    consumer_count: Int,
  )
}

pub type StreamMessage {
  StreamMessage(sequence: Int, time: String, message: Message)
}

//             //
// Stream Info //
//             //

/// Get info about a stream by name.
///
pub fn info(
  conn: Connection,
  name: String,
) -> Result(StreamInfo, JetstreamError) {
  let subject = stream_prefix <> ".INFO." <> name

  case glats.request(conn, subject, "", 1000) {
    Ok(msg) -> decode_info(msg.body)
    Error(_) -> Error(jetstream.StreamNotFound)
  }
}

external fn decode_info_data(
  data: Map(String, Dynamic),
) -> Result(StreamInfo, #(Int, String)) =
  "Elixir.Glats.Jetstream" "decode_info_data"

fn decode_info(body: String) {
  let decoder = dynamic.map(dynamic.string, dynamic.dynamic)

  json.decode(body, decoder)
  |> result.map(decode_info_data)
  |> result.map_error(fn(_) { #(-1, "decode error") })
  |> result.flatten
  |> result.map_error(js.map_code_to_error)
}

//               //
// Create Stream //
//               //

/// Creates a new stream.
///
/// Calling this when a stream by the same already exists will run successfully
/// when no mutable field in the config is different.
///
pub fn create(conn: Connection, stream: StreamConfig) {
  let subject = stream_prefix <> ".CREATE." <> stream.name
  let body = config_to_json(stream)

  case glats.request(conn, subject, body, 1000) {
    Ok(msg) -> decode_info(msg.body)
    // TODO: use actual descriptive error
    Error(_) -> Error(jetstream.StreamNotFound)
  }
}

//               //
// Delete Stream //
//               //

/// Deletes a stream.
///
pub fn delete(conn: Connection, name: String) {
  let subject = stream_prefix <> ".DELETE." <> name

  case glats.request(conn, subject, "", 1000) {
    Ok(msg) -> decode_delete(msg.body)
    // TODO: use actual descriptive error
    Error(_) -> Error(jetstream.StreamNotFound)
  }
}

external fn decode_delete_data(
  data: Map(String, Dynamic),
) -> Result(Nil, #(Int, String)) =
  "Elixir.Glats.Jetstream" "decode_delete_data"

fn decode_delete(body: String) -> Result(Nil, JetstreamError) {
  let decoder = dynamic.map(dynamic.string, dynamic.dynamic)

  json.decode(body, decoder)
  |> result.map(decode_delete_data)
  |> result.map_error(fn(_) { #(-1, "decode error") })
  |> result.flatten
  |> result.map_error(js.map_code_to_error)
}

//             //
// Get Message //
//             //

/// Access method type for a get message request to Jetstream.
///
/// See `get_message".
///`
pub type AccessMethod {
  BySequence(Int)
  BySubject(String)
}

type RawStreamMessage {
  RawStreamMessage(
    subject: String,
    seq: Int,
    hdrs: Option(String),
    data: String,
    time: String,
  )
}

external fn decode_raw_stream_message_data(
  data: Map(String, Dynamic),
) -> Result(RawStreamMessage, #(Int, String)) =
  "Elixir.Glats.Jetstream" "decode_raw_stream_message_data"

/// Directly fetches a message from a stream either by sequence ID
/// (by passing `BySequence(Int)`) or by subject (by passing
/// `BySubject(String)`).
///
/// A subject can have wildcards (e.g. `orders.*.item.>`), please
/// refer to [Subject-Based messaging docs](https://docs.nats.io/nats-concepts/subjects)
/// for more info.
///
/// Keep in mind that `allow_direct` has to be enabled in stream config
/// for this to work.
///
pub fn get_message(conn: Connection, stream: String, method: AccessMethod) {
  let subject = stream_prefix <> ".MSG.GET." <> stream
  let body = encode_get_message_body(method)

  case glats.request(conn, subject, body, 1000) {
    Ok(msg) ->
      decode_raw_message(msg.body)
      |> result.then(fn(d) {
        raw_to_stream_message(d)
        |> result.map_error(fn(_) { jetstream.DecodeError(msg.body) })
      })
    Error(_) -> Error(jetstream.Unknown(-1, "unknown error"))
  }
}

// JSON serializes the body that should be sent in the get message
// request to jetstream.
//
fn encode_get_message_body(method: AccessMethod) -> String {
  case method {
    BySequence(seq) -> [#("seq", json.int(seq))]
    BySubject(subj) -> [#("last_by_subj", json.string(subj))]
  }
  |> json.object
  |> json.to_string
}

// Decode the raw message JSON from a get message request.
//
fn decode_raw_message(body: String) {
  let decoder = dynamic.map(dynamic.string, dynamic.dynamic)

  json.decode(body, decoder)
  |> result.map(decode_raw_stream_message_data)
  |> result.map_error(fn(_) { #(-1, "decode error") })
  |> result.flatten
  |> result.map_error(js.map_code_to_error)
}

// Map a RawStreamMessage to a StreamMessage.
//
fn raw_to_stream_message(msg: RawStreamMessage) {
  use body <- result.then(
    base.decode64(msg.data)
    |> result.map(bit_string.to_string),
  )

  // TODO: decode headers
  Ok(StreamMessage(
    sequence: msg.seq,
    time: msg.time,
    message: Message(
      subject: msg.subject,
      headers: map.new(),
      reply_to: None,
      body: body
      |> result.unwrap(""),
    ),
  ))
}

//               //
// Stream Config //
//               //

/// Creates a new stream config from name and subject list.
///
pub fn new_config(name: String, subjects: List(String)) {
  StreamConfig(
    name: name,
    subjects: subjects,
    retention: None,
    max_consumers: None,
    max_msgs: None,
    max_bytes: None,
    max_age: None,
    max_msgs_per_subject: None,
    max_msg_size: None,
    discard: None,
    storage: None,
    num_replicas: None,
    duplicate_window: None,
    allow_direct: None,
    mirror_direct: None,
    sealed: None,
    deny_delete: None,
    deny_purge: None,
    allow_rollup_hdrs: None,
  )
}

/// Sets the storage type in a stream config.
///
pub fn with_storage(config: StreamConfig, storage: jetstream.StorageType) {
  let st = case storage {
    jetstream.FileStorage -> "file"
    jetstream.MemoryStorage -> "memory"
  }

  StreamConfig(..config, storage: Some(st))
}

/// Sets the retention policy in a stream config.
///
pub fn with_retention(
  config: StreamConfig,
  retention: jetstream.RetentionPolicy,
) {
  let rp = case retention {
    jetstream.LimitsPolicy -> "limits"
    jetstream.InterestPolicy -> "interest"
    jetstream.WorkQueuePolicy -> "workqueue"
  }

  StreamConfig(..config, retention: Some(rp))
}

/// Sets the number of max consumers in a stream config.
///
pub fn with_max_consumers(config: StreamConfig, max_consumers: Int) {
  StreamConfig(..config, max_consumers: Some(max_consumers))
}

pub fn config_to_json(config: StreamConfig) -> String {
  // Start by putting all optionals in a list of tuples.
  [
    #(
      "retention",
      config.retention
      |> option.map(json.string),
    ),
    #(
      "max_consumers",
      config.max_consumers
      |> option.map(json.int),
    ),
    #(
      "max_msgs",
      config.max_msgs
      |> option.map(json.int),
    ),
    #(
      "max_bytes",
      config.max_bytes
      |> option.map(json.int),
    ),
    #(
      "max_age",
      config.max_age
      |> option.map(json.int),
    ),
    #(
      "max_msgs_per_subject",
      config.max_msgs_per_subject
      |> option.map(json.int),
    ),
    #(
      "max_msg_size",
      config.max_msg_size
      |> option.map(json.int),
    ),
    #(
      "discard",
      config.discard
      |> option.map(json.string),
    ),
    #(
      "storage",
      config.storage
      |> option.map(json.string),
    ),
    #(
      "num_replicas",
      config.num_replicas
      |> option.map(json.int),
    ),
    #(
      "duplicate_window",
      config.duplicate_window
      |> option.map(json.int),
    ),
    #(
      "allow_direct",
      config.allow_direct
      |> option.map(json.bool),
    ),
    #(
      "mirror_direct",
      config.mirror_direct
      |> option.map(json.bool),
    ),
    #(
      "sealed",
      config.sealed
      |> option.map(json.bool),
    ),
    #(
      "deny_delete",
      config.deny_delete
      |> option.map(json.bool),
    ),
    #(
      "deny_purge",
      config.deny_purge
      |> option.map(json.bool),
    ),
    #(
      "allow_rollup_hdrs",
      config.allow_rollup_hdrs
      |> option.map(json.bool),
    ),
  ]
  |> list.filter_map(fn(item) {
    case item.1 {
      Some(val) -> Ok(#(item.0, val))
      None -> Error(item)
    }
  })
  |> list.prepend(#("subjects", json.array(config.subjects, json.string)))
  |> list.prepend(#("name", json.string(config.name)))
  |> json.object
  |> json.to_string
}
