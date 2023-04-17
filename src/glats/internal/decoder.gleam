import gleam/map
import gleam/dynamic.{Dynamic}
import gleam/result
import gleam/option.{None}
import gleam/erlang/atom
import glats/message.{Message}

fn atom_field(key: String, value) {
  dynamic.field(atom.create_from_string(key), value)
}

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

pub fn headers(data: Dynamic) {
  data
  |> atom_field(
    "headers",
    dynamic.list(dynamic.tuple2(dynamic.string, dynamic.string)),
  )
  |> result.map(map.from_list)
  |> result.or(Ok(map.new()))
}

pub fn reply_to(data: Dynamic) {
  data
  |> dynamic.optional(atom_field("reply_to", dynamic.string))
  |> result.or(Ok(None))
}
