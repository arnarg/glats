import gleam/io
import gleam/map
import gleam/dynamic.{Dynamic}
import gleam/result
import gleam/option.{None, Option}
import gleam/erlang/atom
import glats/message.{Message}
import glats/protocol.{ServerInfo}

fn atom_field(key: String, value) {
  dynamic.field(atom.create_from_string(key), value)
}

/// Decodes a message map returned by NATS
///
pub fn decode_msg(data: Dynamic) {
  data
  |> dynamic.decode4(
    Message,
    atom_field("topic", dynamic.string),
    headers,
    reply_to,
    atom_field("body", dynamic.string),
  )
}

/// Decodes headers from a map with message data.
/// If the key is absent (which happens when no headers are sent)
/// an empty map is returned.
///
pub fn headers(data: Dynamic) {
  data
  |> atom_field(
    "headers",
    dynamic.list(dynamic.tuple2(dynamic.string, dynamic.string)),
  )
  |> result.map(map.from_list)
  |> result.or(Ok(map.new()))
}

/// Decodes reply_to from a map with message data into Option(String).
/// If reply_to is `Nil` None is returned.
///
pub fn reply_to(data: Dynamic) {
  data
  |> dynamic.optional(atom_field("reply_to", dynamic.string))
  |> result.or(Ok(None))
}

pub type MandatoryFields {
  MandatoryFields(
    server_id: String,
    server_name: String,
    version: String,
    go: String,
    host: String,
    port: Int,
    headers: Bool,
    max_payload: Int,
    proto: Int,
  )
}

pub type OptionalFields {
  OptionalFields(jetstream: Bool, auth_required: Option(Bool))
}

/// Decodes a server info from a map.
pub fn decode_server_info(data: Dynamic) {
  use mandatory <- result.then(decode_mandatory_server_info(data))
  use optional <- result.then(decode_optional_server_info(data))

  Ok(ServerInfo(
    mandatory.server_id,
    mandatory.server_name,
    mandatory.version,
    mandatory.go,
    mandatory.host,
    mandatory.port,
    mandatory.headers,
    mandatory.max_payload,
    mandatory.proto,
    optional.jetstream,
    optional.auth_required,
  ))
}

/// Decodes mandatory fields from server info map.
pub fn decode_mandatory_server_info(data: Dynamic) {
  data
  |> dynamic.decode9(
    MandatoryFields,
    atom_field("server_id", dynamic.string),
    atom_field("server_name", dynamic.string),
    atom_field("version", dynamic.string),
    atom_field("go", dynamic.string),
    atom_field("host", dynamic.string),
    atom_field("port", dynamic.int),
    atom_field("headers", dynamic.bool),
    atom_field("max_payload", dynamic.int),
    atom_field("proto", dynamic.int),
  )
}

/// Decodes optional fields from server info map.
pub fn decode_optional_server_info(data: Dynamic) {
  data
  |> dynamic.decode2(
    OptionalFields,
    jetstream,
    optional_field("auth_required", dynamic.bool),
  )
}

fn jetstream(data: Dynamic) {
  data
  |> atom_field("jetstream", dynamic.bool)
  |> result.or(Ok(False))
}

fn optional_field(key: String, value) {
  fn(data) {
    data
    |> dynamic.optional(atom_field(key, value))
    |> result.or(Ok(None))
  }
}
