import gleam/map
import gleam/dynamic
import gleam/option.{None, Some}
import gleam/erlang/atom
import gleam/erlang/process.{Selector}
import gleeunit/should
import glats/internal/decoder
import glats/message.{Message}

fn default_message() {
  Message(subject: "error", headers: map.new(), reply_to: None, body: "error")
}

pub fn headers_present_test() {
  map.new()
  |> map.insert(atom.create_from_string("headers"), [#("key", "value")])
  |> dynamic.from
  |> decoder.headers
  |> should.equal(Ok(
    [#("key", "value")]
    |> map.from_list,
  ))
}

pub fn headers_absent_test() {
  map.new()
  |> dynamic.from
  |> decoder.headers
  |> should.equal(Ok(map.new()))
}

pub fn reply_to_not_nil_test() {
  map.new()
  |> map.insert(atom.create_from_string("reply_to"), "INBOX.reply")
  |> dynamic.from
  |> decoder.reply_to
  |> should.equal(Ok(Some("INBOX.reply")))
}

pub fn reply_to_nil_test() {
  map.new()
  |> map.insert(atom.create_from_string("reply_to"), Nil)
  |> dynamic.from
  |> decoder.reply_to
  |> should.equal(Ok(None))
}

pub fn decode_msg_full_test() {
  [
    #(atom.create_from_string("topic"), dynamic.from("some.subject")),
    #(atom.create_from_string("headers"), dynamic.from([#("some", "header")])),
    #(atom.create_from_string("reply_to"), dynamic.from("INBOX.reply")),
    #(atom.create_from_string("body"), dynamic.from("Hello world!")),
  ]
  |> map.from_list
  |> dynamic.from
  |> decoder.decode_msg
  |> should.equal(Ok(Message(
    subject: "some.subject",
    headers: map.new()
    |> map.insert("some", "header"),
    reply_to: Some("INBOX.reply"),
    body: "Hello world!",
  )))
}

pub fn decode_msg_no_reply_to_test() {
  [
    #(atom.create_from_string("topic"), dynamic.from("some.subject")),
    #(atom.create_from_string("headers"), dynamic.from([#("some", "header")])),
    #(atom.create_from_string("reply_to"), dynamic.from(Nil)),
    #(atom.create_from_string("body"), dynamic.from("Hello world!")),
  ]
  |> map.from_list
  |> dynamic.from
  |> decoder.decode_msg
  |> should.equal(Ok(Message(
    subject: "some.subject",
    headers: map.new()
    |> map.insert("some", "header"),
    reply_to: None,
    body: "Hello world!",
  )))
}

pub fn decode_msg_no_headers_test() {
  [
    #(atom.create_from_string("topic"), dynamic.from("some.subject")),
    #(atom.create_from_string("reply_to"), dynamic.from("INBOX.reply")),
    #(atom.create_from_string("body"), dynamic.from("Hello world!")),
  ]
  |> map.from_list
  |> dynamic.from
  |> decoder.decode_msg
  |> should.equal(Ok(Message(
    subject: "some.subject",
    headers: map.new(),
    reply_to: Some("INBOX.reply"),
    body: "Hello world!",
  )))
}
