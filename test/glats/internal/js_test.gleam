import gleeunit/should
import glats/jetstream
import glats/internal/js

pub fn stream_not_found_test() {
  js.map_code_to_error(#(10_059, "stream not found"))
  |> should.equal(jetstream.StreamNotFound)
}

pub fn consumer_not_found_test() {
  js.map_code_to_error(#(10_014, "consumer not found"))
  |> should.equal(jetstream.ConsumerNotFound)
}

pub fn unknown_error_test() {
  js.map_code_to_error(#(0, "unknown error"))
  |> should.equal(jetstream.Unknown(code: 0, description: "unknown error"))
}
