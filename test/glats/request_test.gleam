import gleam/map
import gleeunit/should
import glats/request.{Request}

// Check that correct request is created by new.
pub fn new_test() {
  request.new("Hello world!")
  |> should.equal(Request(headers: map.new(), body: "Hello world!"))
}

// Check that set_header sets correct header in request.
pub fn set_header_test() {
  request.new("Hello world!")
  |> request.set_header("key", "value")
  |> should.equal(Request(
    headers: map.from_list([#("key", "value")]),
    body: "Hello world!",
  ))
}
