import gleam/io
import gleam/int
import gleam/result
import glats
import glats/settings
import glats/jetstream.{MemoryStorage, WorkQueuePolicy}
import glats/jetstream/stream.{Retention, Storage}

pub fn main() {
  use conn <- result.then(
    settings.new("localhost", 4222)
    |> glats.connect,
  )

  let assert Ok(created) =
    stream.create(
      conn,
      "mystream",
      ["orders.>", "items.>"],
      [Storage(MemoryStorage), Retention(WorkQueuePolicy)],
    )

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
    stream.get_message(conn, info.config.name, stream.LastBySubject("orders.>"))
    |> io.debug

  let assert Ok(_) =
    stream.get_message(conn, info.config.name, stream.SequenceID(1))
    |> io.debug

  let assert Ok(Nil) =
    stream.delete(conn, created.config.name)
    |> io.debug

  Ok(Nil)
}
