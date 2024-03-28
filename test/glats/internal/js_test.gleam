import gleeunit/should
import gleam/dict
import glats/jetstream
import glats/internal/js

pub fn stream_not_found_test() {
  js.map_code_to_error(#(10_059, "stream not found"))
  |> should.equal(jetstream.StreamNotFound("stream not found"))
}

pub fn consumer_not_found_test() {
  js.map_code_to_error(#(10_014, "consumer not found"))
  |> should.equal(jetstream.ConsumerNotFound("consumer not found"))
}

pub fn unknown_error_test() {
  js.map_code_to_error(#(0, "unknown error"))
  |> should.equal(jetstream.Unknown(0, "unknown error"))
}

pub fn decode_headers_test() {
  "NATS/1.0\nsome: header\nanother: one\n\n"
  |> js.decode_headers
  |> should.equal(Ok(
    [#("some", "header"), #("another", "one")]
    |> dict.from_list,
  ))
}
