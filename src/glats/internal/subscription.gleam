import gleam/io
import gleam/int
import gleam/string
import gleam/map.{Map}
import gleam/option.{None, Option}
import gleam/dynamic.{Dynamic}
import gleam/result
import gleam/function.{constant}
import gleam/erlang/atom
import gleam/erlang/process.{Subject}
import gleam/otp/actor

/// Raw message received from Gnat.
pub type RawMessage {
  RawMessage(
    sid: Int,
    status: Int,
    topic: String,
    headers: Map(String, String),
    reply_to: Option(String),
    body: String,
  )
}

pub type MapperFunc(a, b) =
  fn(a, RawMessage) -> b

type State(a, b) {
  State(conn: a, receiver: Subject(b), mapper: MapperFunc(a, b))
}

pub opaque type Message {
  ReceivedMessage(RawMessage)
  DecodeError(Dynamic)
  SubscriberExited
}

pub fn start_subscriber(conn: a, receiver: Subject(b), mapper: MapperFunc(a, b)) {
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
        |> process.selecting_process_down(monitor, constant(SubscriberExited))
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

fn map_gnat_message(data: Dynamic) -> Message {
  data
  |> decode_raw_msg
  |> result.map(ReceivedMessage)
  |> result.unwrap(DecodeError(data))
}

fn loop(message: Message, state: State(a, b)) {
  case message {
    ReceivedMessage(raw_msg) -> {
      actor.send(state.receiver, state.mapper(state.conn, raw_msg))
      actor.Continue(state)
    }
    DecodeError(data) -> {
      io.println("failed to decode: " <> string.inspect(data))
      actor.Continue(state)
    }
    SubscriberExited -> {
      io.println("subscriber exited")
      actor.Stop(process.Normal)
    }
  }
}

// Decode Gnat message

// Decodes a message map returned by NATS
pub fn decode_raw_msg(data: Dynamic) {
  data
  |> dynamic.decode6(
    RawMessage,
    sid,
    status,
    atom_field("topic", dynamic.string),
    headers,
    reply_to,
    atom_field("body", dynamic.string),
  )
}

// Decodes sid with default value of -1 if not found.
fn sid(data: Dynamic) {
  data
  |> atom_field("sid", dynamic.int)
  |> result.or(Ok(-1))
}

// Decodes status with default value of -1 if not found.
fn status(data: Dynamic) {
  data
  |> atom_field("status", dynamic.string)
  |> result.map(fn(s) {
    int.parse(s)
    |> result.unwrap(-1)
  })
  |> result.or(Ok(-1))
}

// Decodes headers from a map with message data.
// If the key is absent (which happens when no headers are sent)
// an empty map is returned.
fn headers(data: Dynamic) {
  data
  |> atom_field(
    "headers",
    dynamic.list(dynamic.tuple2(dynamic.string, dynamic.string)),
  )
  |> result.map(map.from_list)
  |> result.or(Ok(map.new()))
}

// Decodes reply_to from a map with message data into Option(String).
// If reply_to is `Nil` None is returned.
fn reply_to(data: Dynamic) {
  data
  |> dynamic.optional(atom_field("reply_to", dynamic.string))
  |> result.or(Ok(None))
}

fn atom_field(key: String, value) {
  dynamic.field(atom.create_from_string(key), value)
}
