import gleam/io
import gleam/map
import gleam/result
import gleam/option.{Some}
import gleam/erlang/process
import glats
import glats/message.{Message}
import glats/jetstream/stream
import glats/jetstream/consumer.{DurableName, FilterSubject}

pub fn main() {
  use conn <- result.then(glats.connect("localhost", 4222, []))

  glats.server_info(conn)
  |> io.debug

  let assert Ok(stream) =
    stream.create(conn, "mystream", ["orders.>", "items.>"], [])

  let assert Ok(created) =
    consumer.create(
      conn,
      stream.config.name,
      [DurableName("myconsumer"), FilterSubject("orders.*")],
    )
    |> io.debug

  // let assert Ok(Nil) =
  //   glats.publish(conn, "orders.1", "order_data")
  //   |> io.debug

  // let assert Ok(Nil) =
  //   glats.publish(conn, "orders.2", "order_data")
  //   |> io.debug

  let subject = process.new_subject()

  glats.subscribe(conn, subject, "_INBOX.subscriber")

  glats.publish_message(
    conn,
    Message(
      subject: "$JS.API.CONSUMER.MSG.NEXT.mystream.myconsumer",
      headers: map.new(),
      reply_to: Some("_INBOX.subscriber"),
      body: "1",
    ),
  )

  process.receive(subject, 100_000)
  |> io.debug

  Ok(Nil)
}
