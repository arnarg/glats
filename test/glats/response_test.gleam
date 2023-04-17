import gleam/map
import gleeunit/should
import glats/response.{Response}

// Check that correct response is created by new.
pub fn new_test() {
  response.new("Hello world!")
  |> should.equal(Response(headers: map.new(), body: "Hello world!"))
}

// Check that set_header sets correct header in response.
pub fn set_header_test() {
  response.new("Hello world!")
  |> response.set_header("key", "value")
  |> should.equal(Response(
    headers: map.from_list([#("key", "value")]),
    body: "Hello world!",
  ))
}
