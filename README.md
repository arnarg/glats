# glats

[![Package Version](https://img.shields.io/hexpm/v/glats)](https://hex.pm/packages/glats)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/glats/)

A NATS client for Gleam. This wraps Elixir's client, [Gnat](https://hex.pm/packages/gnat).

## Publish

```gleam
import gleam/result
import glats

pub fn main() {
  use conn <- result.then(glats.connect("localhost", 4222, []))

  // Publish a single message to "some.topic".
  let assert Ok(Nil) = glats.publish(conn, "some.topic", "hello world!", [])

  Ok(Nil)
}
```

## Subscribe

```gleam
import gleam/result
import gleam/erlang/process
import glats

pub fn main() {
  use conn <- result.then(glats.connect("localhost", 4222, []))

  let subject = process.new_subject()

  // Subscribe to "some.topic".
  // Messages will be delivered to the erlang subject passed in.
  let assert Ok(sid) = glats.subscribe(conn, subject, "some.topic", [])

  // Publish a single message to "some.topic".
  let assert Ok(Nil) = glats.publish(conn, "some.topic", "hello world!", [])

  // Receive from erlang subject.
  let assert Ok(glats.ReceivedMessage(
    conn: _conn,
    sid: _sid,
    status: _status,
    message: glats.Message(
      topic: _topic,
      headers: _headers,
      reply_to: _reply_to,
      body: _body,
    ),
  )) = process.receive(subject, 1000)

  // Unsubscribe from the subscription.
  let assert Ok(Nil) = glats.unsubscribe(conn, sid)

  Ok(Nil)
}
```

## Request handler

```gleam
import gleam/io
import gleam/option.{None}
import gleam/result
import gleam/erlang/process
import glats
import glats/handler.{Reply, Request, Response}

pub fn main() {
  use conn <- result.then(glats.connect("localhost", 4222, []))

  // Start a request handler actor that will call `ping_pong_handler`
  // for every request received from NATS topic "do.ping".
  let assert Ok(_actor) =
    handler.handle_request(conn, [], "do.ping", [], ping_pong_handler)

  process.sleep_forever()

  Ok(Nil)
}

pub fn ping_pong_handler(req: Request, state) {
  // Got message: Hello
  io.println("Got message: " <> req.body)

  // Reply with a message with the same headers and append to body.
  Reply(
    Response(
      headers: req.headers,
      reply_to: None,
      body: req.body <> " from glats!",
    ),
    state,
  )
}
```

Then in shell with `natscli`.

```sh
$ nats req do.ping 'Hello'
12:16:47 Sending request on "do.ping"
12:16:47 Received with rtt 427.64µs
Hello from glats!
```

## Jetstream

### Pull subscription

Pull consumers allow clients to request batches of messages on demand.
For best performance set the batch size to as high as possible, but keep
in mind that the client should be able to process (and ack) the entire
batch in the `AckWait` time window of the consumer.

In the example below we request a batch of 50 messages with `NoWait` set.
This instructs NATS to immediately return a response, event when no pending
messages exist in the stream (in which case we get a message with status `404`).
That way we can exit the program when we're caught up.

Normally you would request a batch with an expiry time (option `Expires(Int)`),
in which case the request lasts for the time requested (and a message with status
`408` is received when it expires).

This allows the client to control the flow of messages instead of NATS flooding
it when there is high load as is possible with push consumers.

[More info on pull consumers](https://www.byronruth.com/grokking-nats-consumers-part-3/)

All the logic in this example is handled for you in `glats/jetstream/handler.{handle_pull_consumer}`,
see [docs](https://hexdocs.pm/glats/glats/jetstream/handler.html).

```gleam
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
  NoWait, Subscription, With,
}

const batch_size = 50

type State {
  State(sub: Subscription, remaining: Int)
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
```

### Push subscription

With push consumers messages are automatically published on a
specified subject.

This example is considerably simpler than the pull consumer above
but keep in mind that with push consumers the flow of messages is
harder to control.

[More info on push consumers](https://www.byronruth.com/grokking-nats-consumers-part-1/)

```gleam
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
```

## Installation

```sh
gleam add glats
```

and its documentation can be found at <https://hexdocs.pm/glats>.
