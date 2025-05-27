import gleam/dict
import gleam/dynamic
import gleam/dynamic/decode
import gleam/erlang/atom
import gleam/erlang/process
import gleam/io
import gleam/option.{None}
import gleam/otp/actor
import gleam/result
import gleam/string

/// Raw message received from Gnat.
pub type RawMessage {
  RawMessage(
    sid: Int,
    status: option.Option(Int),
    topic: String,
    headers: dict.Dict(String, String),
    reply_to: option.Option(String),
    body: String,
  )
}

pub type MapperFunc(a, b) =
  fn(a, RawMessage) -> b

type State(a, b) {
  State(conn: a, receiver: process.Subject(b), mapper: MapperFunc(a, b))
}

pub opaque type Message {
  ReceivedMessage(RawMessage)
  DecodeError(dynamic.Dynamic)
  SubscriberExited
}

pub fn start_subscriber(
  conn: a,
  receiver: process.Subject(b),
  mapper: MapperFunc(a, b),
) {
  actor.start_spec(actor.Spec(
    init: fn() {
      // Monitor subscriber process.
      let monitor =
        process.monitor_process(
          receiver
          |> process.subject_owner,
        )

      let selector =
        process.new_selector()
        |> process.selecting_process_down(monitor, fn(_) { SubscriberExited })
        |> process.selecting_record2(
          atom.create_from_string("msg"),
          map_gnat_message,
        )

      actor.Ready(State(conn, receiver, mapper), selector)
    },
    init_timeout: 1000,
    loop: loop,
  ))
}

fn map_gnat_message(data: dynamic.Dynamic) -> Message {
  data
  |> decode_raw_msg
  |> result.map(ReceivedMessage)
  |> result.unwrap(DecodeError(data))
}

fn loop(message: Message, state: State(a, b)) {
  case message {
    ReceivedMessage(raw_msg) -> {
      actor.send(state.receiver, state.mapper(state.conn, raw_msg))
      actor.Continue(state, None)
    }
    DecodeError(data) -> {
      io.println("failed to decode: " <> string.inspect(data))
      actor.Continue(state, None)
    }
    SubscriberExited -> {
      io.println("subscriber exited")
      actor.Stop(process.Normal)
    }
  }
}

// Decode Gnat message

// Decodes a message map returned by NATS
pub fn decode_raw_msg(data: dynamic.Dynamic) {
  let decoder = {
    use sid <- decode_sid
    use status <- decode_status
    use topic <- decode.field(atom.create_from_string("topic"), decode.string)
    use headers <- decode_headers
    use reply_to <- decode_reply_to
    use body <- decode.field(atom.create_from_string("body"), decode.string)

    decode.success(RawMessage(sid, status, topic, headers, reply_to, body))
  }

  decode.run(data, decoder)
}

// Decodes sid with default value of -1 if not found.
fn decode_sid(next) {
  decode.optional_field(atom.create_from_string("sid"), -1, decode.int, next)
}

// Decodes status.
fn decode_status(next) {
  decode.optional_field(
    atom.create_from_string("status"),
    None,
    decode.optional(decode.int),
    next,
  )
}

// Decodes headers from a map with message data.
// If the key is absent (which happens when no headers are sent)
// an empty map is returned.
fn decode_headers(next) {
  decode.optional_field(
    atom.create_from_string("headers"),
    dict.new(),
    decode.dict(decode.string, decode.string),
    next,
  )
}

// Decodes reply_to from a map with message data into option.Option(String).
// If reply_to is `Nil` None is returned.
fn decode_reply_to(next) {
  decode.optional_field(
    atom.create_from_string("reply_to"),
    None,
    decode.optional(decode.string),
    next,
  )
}
