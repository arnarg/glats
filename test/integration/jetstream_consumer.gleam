import gleam/io
import gleam/result
import glats
import glats/settings
import glats/jetstream/stream
import glats/jetstream/consumer.{DurableName, FilterSubject}

pub fn main() {
  use conn <- result.then(
    settings.new("localhost", 4222)
    |> glats.connect,
  )

  let assert Ok(stream) =
    stream.create(conn, "mystream", ["orders.>", "items.>"], [])

  let assert Ok(created) =
    consumer.create(
      conn,
      stream.config.name,
      [DurableName("myconsumer"), FilterSubject("orders.*")],
    )
    |> io.debug

  Ok(Nil)
}
