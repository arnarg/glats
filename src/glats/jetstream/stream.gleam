import glats
import glats/internal/js
import glats/jetstream
import gleam/bit_array
import gleam/dict
import gleam/dynamic
import gleam/dynamic/decode
import gleam/json
import gleam/list
import gleam/option.{None, Some}
import gleam/result

const stream_prefix = "$JS.API.STREAM"

/// Used to set the retention policy of a stream.
///
pub type RetentionPolicy {
  /// Retention based on the various limits that are set
  /// including: `MaxMessages`, `MaxBytes`, `MaxAge`, and
  /// `MaxMessagesPerSubject`. If any of these limits are
  /// set, whichever limit is hit first will cause the
  /// automatic deletion of the respective message(s).
  LimitsPolicy
  /// Retention based on the consumer interest in the
  /// stream and messages. The base case is that there
  /// are zero consumers defined for a stream. If messages
  /// are published to the stream, they will be immediately
  /// deleted so there is no _interest_.
  InterestPolicy
  /// Retention with the typical behavior of a FIFO queue.
  /// Each message can be consumed only once. This is
  /// enforced by only allowing _one_ consumer to be created
  /// for a work-queue stream.
  WorkQueuePolicy
}

/// Used to set the discard policy of a stream.
///
pub type DiscardPolicy {
  /// This policy will delete the oldest messages in order
  /// to maintain the limit. For example, if `MaxAge` is set
  /// to one minute, the server will automatically delete
  /// messages older than one minute with this policy.
  DiscardOld
  /// This policy will reject new messages from being
  /// appended to the stream if it would exceed one of the
  /// limits. An extension to this policy is
  /// `DiscardNewPerSubject` which will apply this policy
  /// on a per-subject basis within the stream.
  DiscardNew
}

/// Available options to set during stream creation and update.
///
pub type StreamOption {
  /// A verbose description of the stream.
  Description(String)
  /// Declares the retention policy for the stream.
  ///
  /// See: https://docs.nats.io/nats-concepts/jetstream/streams#retentionpolicy
  Retention(RetentionPolicy)
  /// How many Consumers can be defined for a given Stream,
  /// -1 for unlimited.
  MaxConsumers(Int)
  /// How many messages may be in a Stream. Adheres to Discard
  /// Policy, removing oldest or refusing new messages if the
  /// Stream exceeds this number of messages.
  MaxMessages(Int)
  /// How many bytes the Stream may contain. Adheres to
  /// Discard Policy, removing oldest or refusing new messages
  /// if the Stream exceeds this size
  MaxBytes(Int)
  /// Maximum age of any message in the Stream, expressed
  /// in nanoseconds.
  MaxAge(Int)
  /// Limits how many messages in the stream to retain per subject.
  MaxMessagesPerSubject(Int)
  /// The largest message that will be accepted by the Stream
  MaxMessageSize(Int)
  /// The behavior of discarding messages when any streams'
  /// limits have been reached.
  ///
  /// See: https://docs.nats.io/nats-concepts/jetstream/streams#discardpolicy
  Discard(DiscardPolicy)
  /// The storage type for stream data.
  Storage(jetstream.StorageType)
  /// How many replicas to keep for each message in a
  /// clustered JetStream, maximum 5.
  NumReplicas(Int)
  /// The window within which to track duplicate messages,
  /// expressed in nanoseconds.
  DuplicateWindow(Int)
  /// If true, and the stream has more than one replica, each
  /// replica will respond to direct get requests for individual
  /// messages, not only the leader.
  AllowDirect(Bool)
  /// If true, and the stream is a mirror, the mirror will
  /// participate in a serving direct get requests for individual
  /// messages from origin stream.
  MirrorDirect(Bool)
  /// Restricts the ability to delete messages from a stream
  /// via the API.
  DenyDelete(Bool)
  /// Restricts the ability to purge messages from a stream
  /// via the API.
  DenyPurge(Bool)
  /// Allows the use of the Nats-Rollup header to replace all
  /// contents of a stream, or subject in a stream, with a
  /// single new message.
  AllowRollup(Bool)
  /// If `True`, applies discard new semantics on a per subject
  /// basis. Requires `DiscardPolicy` to be `DiscardNew` and
  /// the `MaxMessagesPerSubject` to be set.
  DiscardNewPerSubject(Bool)
}

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
    retention: option.Option(String),
    max_consumers: option.Option(Int),
    max_msgs: option.Option(Int),
    max_bytes: option.Option(Int),
    max_age: option.Option(Int),
    max_msgs_per_subject: option.Option(Int),
    max_msg_size: option.Option(Int),
    discard: option.Option(String),
    storage: option.Option(String),
    num_replicas: option.Option(Int),
    duplicate_window: option.Option(Int),
    allow_direct: option.Option(Bool),
    mirror_direct: option.Option(Bool),
    sealed: option.Option(Bool),
    deny_delete: option.Option(Bool),
    deny_purge: option.Option(Bool),
    allow_rollup_hdrs: option.Option(Bool),
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
  StreamMessage(sequence: Int, time: String, message: glats.Message)
}

//             //
// Stream Info //
//             //

/// Get info about a stream by name.
///
pub fn info(
  conn: glats.Connection,
  name: String,
) -> Result(StreamInfo, jetstream.JetstreamError) {
  let topic = stream_prefix <> ".INFO." <> name

  case glats.request(conn, topic, "", [], 1000) {
    Ok(msg) -> decode_info(msg.body)
    Error(_) -> Error(jetstream.StreamNotFound(""))
  }
}

fn decode_info(body: String) {
  let decoder = {
    use created <- decode.field("created", decode.string)
    use config <- decode.field("config", stream_config_decoder())
    use state <- decode.field("state", stream_state_decoder())

    decode.success(StreamInfo(created, config, state))
  }

  json.parse(from: body, using: decoder)
  |> result.map_error(fn(_) { #(-1, "decode error") })
  |> result.map_error(js.map_code_to_error)
}

fn stream_config_decoder() -> decode.Decoder(StreamConfig) {
  use name <- decode.field("name", decode.string)
  use subjects <- decode.field("subjects", decode.list(of: decode.string))
  use retention <- decode_optional_field("retention", decode.string)
  use max_consumers <- decode_optional_field("max_consumers", decode.int)
  use max_msgs <- decode_optional_field("max_msgs", decode.int)
  use max_bytes <- decode_optional_field("max_bytes", decode.int)
  use max_age <- decode_optional_field("max_age", decode.int)
  use max_msgs_per_subject <- decode_optional_field(
    "max_msgs_per_subject",
    decode.int,
  )
  use max_msg_size <- decode_optional_field("max_msg_size", decode.int)
  use discard <- decode_optional_field("discard", decode.string)
  use storage <- decode_optional_field("storage", decode.string)
  use num_replicas <- decode_optional_field("num_replicas", decode.int)
  use duplicate_window <- decode_optional_field("duplicate_window", decode.int)
  use allow_direct <- decode_optional_field("allow_direct", decode.bool)
  use mirror_direct <- decode_optional_field("mirror_direct", decode.bool)
  use sealed <- decode_optional_field("sealed", decode.bool)
  use deny_delete <- decode_optional_field("deny_delete", decode.bool)
  use deny_purge <- decode_optional_field("deny_purge", decode.bool)
  use allow_rollup_hdrs <- decode_optional_field(
    "allow_rollup_hdrs",
    decode.bool,
  )

  decode.success(StreamConfig(
    name,
    subjects,
    retention,
    max_consumers,
    max_msgs,
    max_bytes,
    max_age,
    max_msgs_per_subject,
    max_msg_size,
    discard,
    storage,
    num_replicas,
    duplicate_window,
    allow_direct,
    mirror_direct,
    sealed,
    deny_delete,
    deny_purge,
    allow_rollup_hdrs,
  ))
}

fn stream_state_decoder() -> decode.Decoder(StreamState) {
  use messages <- decode.field("messages", decode.int)
  use bytes <- decode.field("bytes", decode.int)
  use first_seq <- decode.field("first_seq", decode.int)
  use first_ts <- decode.field("first_ts", decode.string)
  use last_seq <- decode.field("last_seq", decode.int)
  use last_ts <- decode.field("last_ts", decode.string)
  use consumer_count <- decode.field("consumer_count", decode.int)

  decode.success(StreamState(
    messages,
    bytes,
    first_seq,
    first_ts,
    last_seq,
    last_ts,
    consumer_count,
  ))
}

fn decode_optional_field(key: String, field_decoder: decode.Decoder(a), next) {
  decode.optional_field(key, None, decode.optional(field_decoder), next)
}

//               //
// Create Stream //
//               //

/// Creates a new stream.
///
/// Calling this when a stream by the same already exists will run successfully
/// when no mutable field in the config is different.
///
/// ## Example
///
/// ```gleam
/// create(
///   conn,
///   "samplestream",
///   ["sample.subject.>"],
///   [
///     Storage(MemoryStorage),
///     Retention(WorkQueuePolicy),
///   ],
/// )
/// ```
pub fn create(
  conn: glats.Connection,
  name: String,
  subjects: List(String),
  opts: List(StreamOption),
) {
  let topic = stream_prefix <> ".CREATE." <> name
  let body =
    [
      #("name", json.string(name)),
      #("subjects", json.array(subjects, of: json.string)),
    ]
    |> stream_options_to_json(opts)

  case glats.request(conn, topic, body, [], 1000) {
    Ok(msg) -> decode_info(msg.body)
    // TODO: use actual descriptive error
    Error(_) -> Error(jetstream.StreamNotFound(""))
  }
}

//               //
// Update Stream //
//               //

/// Updates the config of a stream.
///
pub fn update(conn: glats.Connection, name: String, opts: List(StreamOption)) {
  let topic = stream_prefix <> ".UPDATE." <> name
  let body =
    [#("name", json.string(name))]
    |> stream_options_to_json(opts)

  case glats.request(conn, topic, body, [], 1000) {
    Ok(msg) -> decode_info(msg.body)
    // TODO: use actual descriptive error
    Error(_) -> Error(jetstream.StreamNotFound(""))
  }
}

//               //
// Delete Stream //
//               //

/// Deletes a stream.
///
pub fn delete(conn: glats.Connection, name: String) {
  let topic = stream_prefix <> ".DELETE." <> name

  case glats.request(conn, topic, "", [], 1000) {
    Ok(msg) -> decode_delete(msg.body)
    // TODO: use actual descriptive error
    Error(_) -> Error(jetstream.StreamNotFound(""))
  }
}

fn decode_delete(body: String) -> Result(Nil, jetstream.JetstreamError) {
  let decoder = {
    use _ <- decode.field("success", decode.bool)

    decode.success(Nil)
  }

  body
  |> json_parse_jetstream_errorable(decoder)
}

//              //
// Purge Stream //
//              //

/// Purges all of the data in a Stream, leaves the Stream.
///
pub fn purge(conn: glats.Connection, name: String) {
  let topic = stream_prefix <> ".PURGE." <> name

  case glats.request(conn, topic, "", [], 1000) {
    Ok(msg) -> decode_purge(msg.body)
    // TODO: use actual descriptive error
    Error(_) -> Error(jetstream.StreamNotFound(""))
  }
}

fn decode_purge(body: String) -> Result(Int, jetstream.JetstreamError) {
  body
  |> json_parse_jetstream_errorable({
    use _ <- decode.field("success", decode.bool)
    use purged <- decode.field("purged", decode.int)

    decode.success(purged)
  })
}

//                             //
// Find Stream Name By Subject //
//                             //

/// Tries to find a stream name by subject.
///
pub fn find_stream_name_by_subject(
  conn: glats.Connection,
  subject: String,
) -> Result(String, jetstream.JetstreamError) {
  let topic = stream_prefix <> ".NAMES"

  let body =
    [#("subject", json.string(subject))]
    |> json.object
    |> json.to_string

  case glats.request(conn, topic, body, [], 1000) {
    Ok(msg) ->
      case decode_names(msg.body) {
        Ok(names) ->
          list.first(names)
          |> result.map_error(fn(_) {
            jetstream.StreamNotFound("stream not found")
          })
        Error(err) -> Error(err)
      }
    Error(err) ->
      js.map_glats_error_to_jetstream(err)
      |> Error
  }
}

fn decode_names(body: String) -> Result(List(String), jetstream.JetstreamError) {
  body
  |> json_parse_jetstream_errorable({
    use streams <- decode.field(
      "streams",
      decode.optional(decode.list(decode.string)),
    )

    decode.success(streams |> option.unwrap([]))
  })
}

//                   //
// Get glats.Message //
//                   //

/// Access method type for a get message request to Jetstream.
///
/// See `get_message`.
///
pub type AccessMethod {
  /// Used to get a message from a stream by sequence ID.
  ///
  SequenceID(Int)
  /// Used to get newest message from a stream that matches
  /// a subject.
  ///
  LastBySubject(String)
}

type RawStreamMessage {
  RawStreamMessage(
    topic: String,
    seq: Int,
    hdrs: option.Option(String),
    data: String,
    time: String,
  )
}

/// Directly fetches a message from a stream either by sequence ID
/// (by passing `SequenceID(Int)`) or by subject (by passing
/// `LastBySubject(String)`).
///
/// A subject can have wildcards (e.g. `orders.*.item.>`), please
/// refer to [Subject-Based messaging docs](https://docs.nats.io/nats-concepts/subjects)
/// for more info.
///
/// Keep in mind that `allow_direct` has to be enabled in stream config
/// for this to work.
///
pub fn get_message(conn: glats.Connection, stream: String, method: AccessMethod) {
  let topic = stream_prefix <> ".MSG.GET." <> stream
  let body = encode_get_message_body(method)

  case glats.request(conn, topic, body, [], 1000) {
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
    SequenceID(seq) -> [#("seq", json.int(seq))]
    LastBySubject(subj) -> [#("last_by_subj", json.string(subj))]
  }
  |> json.object
  |> json.to_string
}

// Decode the raw message JSON from a get message request.
//
fn decode_raw_message(body: String) {
  body
  |> json_parse_jetstream_errorable({
    use message <- decode.field("message", {
      use subject <- decode.field("subject", decode.string)
      use seq <- decode.field("seq", decode.int)
      use data <- decode.field("data", decode.string)
      use time <- decode.field("time", decode.string)
      use hdrs <- decode.optional_field(
        "hdrs",
        None,
        decode.optional(decode.string),
      )

      decode.success(RawStreamMessage(topic: subject, seq:, hdrs:, data:, time:))
    })

    decode.success(message)
  })
}

// Map a RawStreamMessage to a StreamMessage.
//
fn raw_to_stream_message(msg: RawStreamMessage) {
  use body <- result.then(
    bit_array.base64_decode(msg.data)
    |> result.map(bit_array.to_string),
  )

  let hdrs = case msg.hdrs {
    Some(data) ->
      js.decode_b64_headers(data)
      |> result.unwrap(dict.new())
    None -> dict.new()
  }

  Ok(StreamMessage(
    sequence: msg.seq,
    time: msg.time,
    message: glats.Message(
      topic: msg.topic,
      headers: hdrs,
      reply_to: None,
      body: body
        |> result.unwrap(""),
    ),
  ))
}

//                  //
// Decoding helpers //
//                  //
fn json_parse_jetstream_errorable(
  body: String,
  decoder: decode.Decoder(t),
) -> Result(t, jetstream.JetstreamError) {
  json.parse(
    from: body,
    using: decode.one_of(decoder |> decode.map(Ok), [
      {
        use err_code <- decode.subfield(["error", "err_code"], decode.int)
        use description <- decode.subfield(
          ["error", "description"],
          decode.string,
        )

        decode.success(Error(#(err_code, description)))
      },
    ]),
  )
  |> result.map_error(fn(_) { #(-1, "decode error") })
  |> result.flatten
  |> result.map_error(js.map_code_to_error)
}

//                                //
// Stream config building helpers //
//                                //

fn stream_options_to_json(
  prev: List(#(String, json.Json)),
  opts: List(StreamOption),
) {
  prev
  |> apply_stream_options(opts)
  |> json.object
  |> json.to_string
}

fn apply_stream_options(
  prev: List(#(String, json.Json)),
  opts: List(StreamOption),
) {
  list.fold(opts, prev, apply_stream_option)
}

fn apply_stream_option(prev: List(#(String, json.Json)), opt: StreamOption) {
  let pair = case opt {
    Description(desc) -> #("description", json.string(desc))
    Retention(pol) -> #(
      "retention",
      ret_pol_to_string(pol)
        |> json.string,
    )
    Discard(pol) -> #(
      "discard",
      dis_pol_to_string(pol)
        |> json.string,
    )
    Storage(storage) -> #(
      "storage",
      storage_to_string(storage)
        |> json.string,
    )
    MaxConsumers(num) -> #("max_consumers", json.int(num))
    MaxMessages(num) -> #("max_msgs", json.int(num))
    MaxBytes(num) -> #("max_bytes", json.int(num))
    MaxAge(num) -> #("max_age", json.int(num))
    MaxMessagesPerSubject(num) -> #("max_msgs_per_subject", json.int(num))
    MaxMessageSize(num) -> #("max_msg_size", json.int(num))
    NumReplicas(num) -> #("num_replicas", json.int(num))
    DuplicateWindow(num) -> #("duplicate_window", json.int(num))
    AllowDirect(val) -> #("allow_direct", json.bool(val))
    MirrorDirect(val) -> #("mirror_direct", json.bool(val))
    DenyDelete(val) -> #("deny_delete", json.bool(val))
    DenyPurge(val) -> #("deny_purge", json.bool(val))
    AllowRollup(val) -> #("allow_rollup_hdrs", json.bool(val))
    DiscardNewPerSubject(val) -> #("discard_new_per_subject", json.bool(val))
  }

  list.prepend(prev, pair)
}

fn ret_pol_to_string(pol: RetentionPolicy) {
  case pol {
    LimitsPolicy -> "limits"
    InterestPolicy -> "interest"
    WorkQueuePolicy -> "workqueue"
  }
}

fn dis_pol_to_string(pol: DiscardPolicy) {
  case pol {
    DiscardOld -> "old"
    DiscardNew -> "new"
  }
}

fn storage_to_string(storage: jetstream.StorageType) {
  case storage {
    jetstream.FileStorage -> "file"
    jetstream.MemoryStorage -> "memory"
  }
}
