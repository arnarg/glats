import gleam/io
import gleam/int
import gleam/result
import glats
import glats/settings
import glats/jetstream
import glats/jetstream/stream

pub fn main() {
  use conn <- result.then(
    settings.new("localhost", 4222)
    |> glats.connect,
  )

  let assert Ok(created) =
    stream.new_config("my_stream", ["orders.>", "items.>"])
    |> stream.with_storage(jetstream.MemoryStorage)
    |> stream.with_retention(jetstream.WorkQueuePolicy)
    |> stream.create(conn, _)

  let assert Ok(info) = stream.info(conn, created.config.name)

  io.println("Stream: " <> info.config.name)
  io.println("Created: " <> info.created)
  io.println("Messages: " <> int.to_string(info.state.messages))

  let assert Ok(Nil) =
    glats.publish(conn, "orders.1", "order_data")
    |> io.debug

  let assert Ok(Nil) =
    glats.publish(conn, "orders.2", "order_data")
    |> io.debug

  let assert Ok(_) =
    stream.get_message(conn, info.config.name, stream.BySubject("orders.>"))
    |> io.debug

  let assert Ok(_) =
    stream.get_message(conn, info.config.name, stream.BySequence(1))
    |> io.debug

  let assert Ok(Nil) =
    stream.delete(conn, created.config.name)
    |> io.debug

  Ok(Nil)
}
