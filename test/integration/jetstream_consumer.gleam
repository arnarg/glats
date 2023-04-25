import gleam/io
import gleam/result
import gleam/erlang/process
import glats
import glats/jetstream
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

  let assert Ok(Nil) =
    glats.publish(conn, "orders.1", "order_data")
    |> io.debug

  // let assert Ok(Nil) =
  //   glats.publish(conn, "orders.2", "order_data")
  //   |> io.debug

  let subject = process.new_subject()

  let assert Ok(sub) =
    consumer.subscribe(
      conn,
      subject,
      "orders.*",
      [
        consumer.With(consumer.Description(
          "An ephemeral consumer for subscription",
        )),
        consumer.With(consumer.AckPolicy(consumer.AckExplicit)),
      ],
    )
    |> io.debug

  consumer.request_next_message(conn, sub, [consumer.NoWait])
  |> io.debug

  // glats.subscribe(conn, subject, "_INBOX.my.subscription")

  // consumer.request_next_message(
  //   conn,
  //   "mystream",
  //   "myconsumer",
  //   "_INBOX.my.subscription",
  //   [consumer.NoWait],
  // )
  // |> io.debug

  let assert Ok(msg) =
    process.receive(subject, 10_000)
    |> io.debug

  jetstream.ack(conn, msg.message)
  |> io.debug

  Ok(Nil)
}
