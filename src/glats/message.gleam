import gleam/map.{Map}
import gleam/option.{None, Option}

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
pub fn new_message(subject: String, message: String) {
  Message(subject: subject, headers: map.new(), reply_to: None, body: message)
}
