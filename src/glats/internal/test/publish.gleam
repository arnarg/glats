import gleam/result
import glats
import glats/settings

pub fn main() {
  use conn <- result.then(
    settings.new("localhost", 4222)
    |> glats.connect,
  )

  let assert Ok(Nil) = glats.publish(conn, "some.subject", "Hello world!")

  Ok(Nil)
}
