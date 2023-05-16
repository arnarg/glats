import gleam/result
import glats

pub fn main() {
  use conn <- result.then(glats.connect("localhost", 4222, []))

  // Publish a single message to "some.subject".
  let assert Ok(Nil) = glats.publish(conn, "some.subject", "hello world!", [])

  Ok(Nil)
}
