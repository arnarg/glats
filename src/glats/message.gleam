import gleam/map.{Map}
import gleam/option.{None, Option, Some}

/// A single message that can be received from or sent to NATS.
pub type Message {
  Message(
    subject: String,
    headers: Map(String, String),
    reply_to: Option(String),
    body: String,
  )
}

/// Constructs a new message.
pub fn new(subject: String, body: String) {
  Message(subject: subject, headers: map.new(), reply_to: None, body: body)
}

/// Sets the reply_to subject of a message.
pub fn set_reply_to(message: Message, reply_to: String) -> Message {
  Message(..message, reply_to: Some(reply_to))
}

/// Sets a header in a message.
pub fn set_header(message: Message, key: String, value: String) {
  Message(
    ..message,
    headers: message.headers
    |> map.insert(key, value),
  )
}
