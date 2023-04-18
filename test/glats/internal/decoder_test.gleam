import gleam/map
import gleam/dynamic
import gleam/option.{None, Some}
import gleam/erlang/atom
import gleeunit/should
import glats/internal/decoder
import glats/message.{Message}
import glats/protocol.{ServerInfo}

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

pub fn decode_mandatory_server_info_test() {
  [
    #(atom.create_from_string("server_id"), dynamic.from("server_id")),
    #(atom.create_from_string("server_name"), dynamic.from("server_name")),
    #(atom.create_from_string("version"), dynamic.from("v1.2.3")),
    #(atom.create_from_string("go"), dynamic.from("v1.2.3")),
    #(atom.create_from_string("host"), dynamic.from("127.0.0.1")),
    #(atom.create_from_string("port"), dynamic.from(4222)),
    #(atom.create_from_string("headers"), dynamic.from(True)),
    #(atom.create_from_string("max_payload"), dynamic.from(1024)),
    #(atom.create_from_string("proto"), dynamic.from(1)),
  ]
  |> map.from_list
  |> dynamic.from
  |> decoder.decode_mandatory_server_info
  |> should.equal(Ok(decoder.MandatoryFields(
    server_id: "server_id",
    server_name: "server_name",
    version: "v1.2.3",
    go: "v1.2.3",
    host: "127.0.0.1",
    port: 4222,
    headers: True,
    max_payload: 1024,
    proto: 1,
  )))
}

pub fn decode_optional_server_info_test() {
  [
    #(atom.create_from_string("jetstream"), dynamic.from(True)),
    #(atom.create_from_string("auth_required"), dynamic.from(True)),
  ]
  |> map.from_list
  |> dynamic.from
  |> decoder.decode_optional_server_info
  |> should.equal(Ok(decoder.OptionalFields(
    jetstream: True,
    auth_required: Some(True),
  )))
}

pub fn decode_optional_server_info_missing_test() {
  map.new()
  |> dynamic.from
  |> decoder.decode_optional_server_info
  |> should.equal(Ok(decoder.OptionalFields(
    jetstream: False,
    auth_required: None,
  )))
}

pub fn decode_server_info_test() {
  [
    #(atom.create_from_string("server_id"), dynamic.from("server_id")),
    #(atom.create_from_string("server_name"), dynamic.from("server_name")),
    #(atom.create_from_string("version"), dynamic.from("v1.2.3")),
    #(atom.create_from_string("go"), dynamic.from("v1.2.3")),
    #(atom.create_from_string("host"), dynamic.from("127.0.0.1")),
    #(atom.create_from_string("port"), dynamic.from(4222)),
    #(atom.create_from_string("headers"), dynamic.from(True)),
    #(atom.create_from_string("max_payload"), dynamic.from(1024)),
    #(atom.create_from_string("proto"), dynamic.from(1)),
    #(atom.create_from_string("jetstream"), dynamic.from(True)),
  ]
  |> map.from_list
  |> dynamic.from
  |> decoder.decode_server_info
  |> should.equal(Ok(ServerInfo(
    server_id: "server_id",
    server_name: "server_name",
    version: "v1.2.3",
    go: "v1.2.3",
    host: "127.0.0.1",
    port: 4222,
    headers: True,
    max_payload: 1024,
    proto: 1,
    jetstream: True,
    auth_required: None,
  )))
}
