import gleam/io
import gleam/result
import gleam/erlang/process.{Subject}
import glats.{ReceivedMessage, SubscriptionMessage}
import glats/jetstream
import glats/jetstream/stream
import glats/jetstream/consumer.{
  AckExplicit, AckPolicy, BindStream, DeliverSubject, Description,
  InactiveThreshold, With,
}

pub fn main() {
  use conn <- result.then(glats.connect("localhost", 4222, []))

  // Create a stream
  let assert Ok(stream) =
    stream.create(conn, "mystream", ["orders.>", "items.>"], [])

  // Publish 3 messages to subjects contained in the stream
  let assert Ok(Nil) = glats.publish(conn, "orders.1", "order_data", [])
  let assert Ok(Nil) = glats.publish(conn, "orders.2", "order_data", [])
  let assert Ok(Nil) = glats.publish(conn, "items.1", "item_data", [])

  // Generate a random inbox topic for the push consumer's delivery topic
  let inbox = glats.new_inbox()

  // Subscribe to subject in the stream using an ephemeral consumer
  let subject = process.new_subject()
  let assert Ok(_sub) =
    consumer.subscribe(
      conn,
      subject,
      "orders.*",
      [
        // Bind to stream created above
        BindStream(stream.config.name),
        // Make it a push consumer
        With(DeliverSubject(inbox)),
        // Set description for the ephemeral consumer
        With(Description("An ephemeral consumer for subscription")),
        // Set ack policy for the consumer
        With(AckPolicy(AckExplicit)),
        // Sets the inactive threshold of the ephemeral consumer
        With(InactiveThreshold(60_000_000_000)),
      ],
    )
    |> io.debug

  // Start loop
  loop(subject)
}

fn loop(subject: Subject(SubscriptionMessage)) {
  case process.receive(subject, 2000) {
    // New message received
    Ok(ReceivedMessage(conn: conn, message: msg, ..)) -> {
      // Print message
      io.debug(msg)

      // Acknowledge message
      let assert Ok(Nil) = jetstream.ack(conn, msg)

      // Run loop again
      loop(subject)
    }

    // Error!
    Error(Nil) -> {
      io.println("no new message in 2 seconds")

      Ok(Nil)
    }
  }
}
