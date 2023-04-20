import gleam/dynamic.{Dynamic}
import gleam/map.{Map}
import gleam/result
import gleam/json
import glats.{Connection}

const stream_prefix = "$JS.API.STREAM"

pub type StreamInfo {
  StreamInfo(created: String, config: StreamConfig, state: StreamState)
}

pub type StreamConfig {
  StreamConfig(
    name: String,
    subjects: List(String),
    retention: String,
    max_consumers: Int,
    max_msgs: Int,
    max_bytes: Int,
    max_age: Int,
    max_msgs_per_subject: Int,
    max_msg_size: Int,
    discard: String,
    storage: String,
    num_replicas: Int,
    duplicate_window: Int,
    allow_direct: Bool,
    mirror_direct: Bool,
    sealed: Bool,
    deny_delete: Bool,
    deny_purge: Bool,
    allow_rollup_hdrs: Bool,
  )
}

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

pub type StreamError {
  StreamNotFound
  DecodeError
}

pub fn info(conn: Connection, name: String) {
  let subject = stream_prefix <> ".INFO." <> name

  case glats.request(conn, subject, "", 1000) {
    Ok(msg) -> decode_info(msg.body)
    Error(_) -> Error(StreamNotFound)
  }
}

external fn decode_info_data(
  data: Map(String, Dynamic),
) -> Result(StreamInfo, StreamError) =
  "Elixir.Glats.Jetstream" "decode_info_data"

fn decode_info(body: String) {
  let decoder = dynamic.map(dynamic.string, dynamic.dynamic)

  json.decode(body, decoder)
  |> result.map(decode_info_data)
  |> result.map_error(fn(_) { DecodeError })
  |> result.flatten
}
