import gleam/io
import gleam/string
import gleam/result
import gleam/option.{Some}
import gleam/otp/actor.{InitFailed}
import gleam/erlang/process.{Abnormal}
import glats.{ReceivedMessage}
import glats/jetstream
import glats/jetstream/stream
import glats/jetstream/consumer.{
  AckExplicit, AckPolicy, Batch, BindStream, Description, InactiveThreshold,
  NoWait, With,
}

const batch_size = 50

type State {
  State(sub: consumer.Subscription, remaining: Int)
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

  // Subscribe to subject in the stream using an ephemeral consumer
  let subject = process.new_subject()
  let assert Ok(sub) =
    consumer.subscribe(conn, subject, "orders.*", [
      // Bind to stream created above
      BindStream(stream.config.name),
      // Set description for the ephemeral consumer
      With(Description("An ephemeral consumer for subscription")),
      // Set ack policy for the consumer
      With(AckPolicy(AckExplicit)),
      // Sets the inactive threshold of the ephemeral consumer
      With(InactiveThreshold(60_000_000_000)),
    ])
    |> io.debug

  // Start loop
  case request_and_loop(subject, State(sub: sub, remaining: 0)) {
    Ok(Nil) -> Ok(Nil)
    Error(err) -> {
      io.println("got error: " <> string.inspect(err))

      Error(InitFailed(Abnormal(string.inspect(err))))
    }
  }
}

// Requests a batch of messages for the pull consumer and updates state
fn request_and_loop(subject, state: State) {
  case consumer.request_batch(state.sub, [Batch(batch_size), NoWait]) {
    Ok(Nil) -> loop(subject, State(..state, remaining: batch_size))
    Error(_) -> Error(Nil)
  }
}

// Receives messages
fn loop(subject, state: State) {
  case process.receive(subject, 2000) {
    // When request expires we get a message with status of `408`.
    Ok(ReceivedMessage(status: Some(408), ..)) -> {
      io.println("request expired, requesting more")

      request_and_loop(subject, state)
    }

    // When request is made with `NoWait` we get a message with status
    // of `404` when no new messages exist.
    Ok(ReceivedMessage(status: Some(404), ..)) -> {
      io.println("no new messages")

      Ok(Nil)
    }

    // We got a new message!
    Ok(ReceivedMessage(conn: conn, message: msg, ..)) -> {
      // Print message
      io.debug(msg)

      // Acknowledge message
      let assert Ok(_) = jetstream.ack(conn, msg)

      // Keep track of remaining messages in the request
      case state.remaining <= 1 {
        // Request more
        True -> request_and_loop(subject, state)
        // Decrement the counter
        False -> loop(subject, State(..state, remaining: state.remaining - 1))
      }
    }

    // An error!
    Error(err) -> Error(err)
  }
}
