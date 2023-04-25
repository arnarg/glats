import gleam/io
import gleam/result
import gleam/option.{None}
import gleam/erlang/process
import glats.{Message, ReceivedMessage}
import glats/jetstream
import glats/jetstream/stream
import glats/jetstream/consumer.{
  AckExplicit, AckPolicy, BindStream, Description, InactiveThreshold, NoWait,
  With,
}

pub fn main() {
  use conn <- result.then(glats.connect("localhost", 4222, []))

  let assert Ok(stream) =
    stream.create(conn, "mystream", ["orders.>", "items.>"], [])

  let assert Ok(Nil) = glats.publish(conn, "orders.1", "order_data")
  let assert Ok(Nil) = glats.publish(conn, "orders.2", "order_data")
  let assert Ok(Nil) = glats.publish(conn, "items.1", "item_data")

  let subject = process.new_subject()

  let assert Ok(sub) =
    consumer.subscribe(
      conn,
      subject,
      "orders.*",
      [
        // Bind to stream created above
        BindStream(stream.config.name),
        // Set description for the ephemeral consumer
        With(Description("An ephemeral consumer for subscription")),
        // Set ack policy for the consumer
        With(AckPolicy(AckExplicit)),
        // Sets the inactive threshold of the ephemeral consumer
        With(InactiveThreshold(60_000_000_000)),
      ],
    )
    |> io.debug

  loop(subject, sub)
}

fn loop(subject, sub: consumer.Subscription) {
  let assert Ok(Nil) = consumer.request_next_message(sub, [NoWait])

  let assert Ok(msg) = process.receive(subject, 1000)

  case msg {
    ReceivedMessage(message: Message(reply_to: None, ..), ..) -> {
      io.println("end of stream")
      Ok(Nil)
    }
    _ -> {
      // Print message contents
      io.debug(msg)
      // Ack message
      let assert Ok(Nil) = jetstream.ack(msg.conn, msg.message)
      // Run loop again
      loop(subject, sub)
    }
  }
}
