import gleam/io
import gleam/int
import gleam/result
import glats
import glats/settings
import glats/jetstream/stream

pub fn main() {
  use conn <- result.then(
    settings.new("localhost", 4222)
    |> glats.connect,
  )

  let assert Ok(info) = stream.info(conn, "test")

  io.println("Stream: " <> info.config.name)
  io.println("Created: " <> info.created)
  io.println("Messages: " <> int.to_string(info.state.messages))

  Ok(Nil)
}
