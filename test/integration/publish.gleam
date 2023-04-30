import gleam/result
import glats

pub fn main() {
  use conn <- result.then(glats.connect("localhost", 4222, []))

  let assert Ok(Nil) = glats.publish(conn, "some.subject", "Hello world!", [])

  Ok(Nil)
}
