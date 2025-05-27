import glats
import glats/jetstream.{MemoryStorage}
import glats/jetstream/stream.{Retention, Storage, WorkQueuePolicy}
import gleam/int
import gleam/io
import gleam/result

pub fn main() {
  use conn <- result.then(glats.connect("localhost", 4222, []))

  let assert Ok(created) =
    stream.create(conn, "mystream", ["orders.>", "items.>"], [
      Storage(MemoryStorage),
      Retention(WorkQueuePolicy),
    ])

  let assert Ok(info) = stream.info(conn, created.config.name)

  let assert Ok("mystream") =
    stream.find_stream_name_by_subject(conn, "orders.*")
    |> echo

  let assert Error(jetstream.StreamNotFound(_)) =
    stream.find_stream_name_by_subject(conn, "nonexisting.*")
    |> echo

  io.println("Stream: " <> info.config.name)
  io.println("Created: " <> info.created)
  io.println("Messages: " <> int.to_string(info.state.messages))

  let assert Ok(Nil) =
    glats.publish(conn, "orders.1", "order_data", [])
    |> echo

  let assert Ok(Nil) =
    glats.publish(conn, "orders.2", "order_data", [])
    |> echo

  let assert Ok(_) =
    stream.get_message(conn, info.config.name, stream.LastBySubject("orders.>"))
    |> echo

  let assert Ok(_) =
    stream.get_message(conn, info.config.name, stream.SequenceID(1))
    |> echo

  let assert Ok(Nil) =
    stream.delete(conn, created.config.name)
    |> echo

  Ok(Nil)
}
