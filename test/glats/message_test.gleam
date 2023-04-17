import gleam/map
import gleam/option.{None, Some}
import gleeunit/should
import glats/message.{Message}

// Check that correct subject and message is created in new.
pub fn new_test() {
  message.new("my.subject", "Hello world!")
  |> should.equal(Message(
    subject: "my.subject",
    headers: map.new(),
    reply_to: None,
    body: "Hello world!",
  ))
}

// Check that set_reply_to correctly sets the reply_to subject.
pub fn set_reply_to_test() {
  message.new("my.subject", "Hello world!")
  |> message.set_reply_to("INBOX.reply_here")
  |> should.equal(Message(
    subject: "my.subject",
    headers: map.new(),
    reply_to: Some("INBOX.reply_here"),
    body: "Hello world!",
  ))
}

// Check that set_header sets correct header in request.
pub fn set_header_test() {
  message.new("my.subject", "Hello world!")
  |> message.set_header("key", "value")
  |> should.equal(Message(
    subject: "my.subject",
    headers: map.from_list([#("key", "value")]),
    reply_to: None,
    body: "Hello world!",
  ))
}
