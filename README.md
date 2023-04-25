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

  // Publish a single message to "some.subject".
  assert Ok(Nil) = glats.publish(conn, "some.subject", "hello world!")

  Ok(Nil)
}
```

## Subscribe

```gleam
import gleam/io
import gleam/result
import gleam/erlang/process
import glats

pub fn main() {
  use conn <- result.then(glats.connect("localhost", 4222, []))

  let subject = process.new_subject()

  // Subscribe to "some.subject".
  // Messages will be delivered to the erlang subject passed in.
  assert Ok(sid) = glats.subscribe(conn, subject, "some.subject")

  // Publish a single message to "some.subject".
  assert Ok(Nil) = glats.publish(conn, "some.subject", "hello world!")

  // Receive from erlang subject.
  assert Ok(glats.ReceivedMessage(
    conn: _conn, // Reference to the conn used
    sid: _sid,   // Subscription ID for the subscription
    message: glats.Message(
      subject: _subject,   // "some.subject"
      headers: _headers,   // empty map
      reply_to: _reply_to, // None
      body: _body,         // "hello world!"
    )
  )) = process.receive(subject, 1000)

  // Unsubscribe from the subscription.
  assert Ok(Nil) = glats.unsubscribe(conn, sid)

  Ok(Nil)
}
```

## Request handler

```gleam
import gleam/option.{None}
import gleam/result
import gleam/erlang/process
import glats
import glats/handler.{Reply, Request, Response}

pub fn main() {
  use conn <- result.then(glats.connect("localhost", 4222, []))

  // Start a request handler actor that will call `ping_pong_handler`
  // for every request received from NATS subject "do.ping".
  assert Ok(_actor) =
    handler.handle_request(conn, [], "do.ping", None, ping_pong_handler)

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

```gleam
import gleam/io
import gleam/result
import gleam/option.{None}
import gleam/erlang/process
import glats.{Message, ReceivedMessage}
import glats/jetstream
import glats/jetstream/stream
import glats/jetstream/consumer.{
  AckExplicit, AckPolicy, BindStream, Description, InactiveThreshold,
  NoWait, With,
}

pub fn main() {
  use conn <- result.then(glats.connect("localhost", 4222, []))

  // Create a stream
  let assert Ok(stream) =
    stream.create(conn, "mystream", ["orders.>", "items.>"], [])

  // Publish 3 messages to subjects contained in the stream
  let assert Ok(Nil) = glats.publish(conn, "orders.1", "order_data")
  let assert Ok(Nil) = glats.publish(conn, "orders.2", "order_data")
  let assert Ok(Nil) = glats.publish(conn, "items.1", "item_data")

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
        // to 1 minute
        With(InactiveThreshold(60_000_000_000)),
      ],
    )
    |> io.debug

  loop(subject, sub)
}

fn loop(subject, sub: consumer.Subscription) {
  // Request the next message from the consumer
  let assert Ok(Nil) =
    consumer.request_next_message(sub.conn, sub, [NoWait])

  // Receive the next message from the erlang subject
  let assert Ok(msg) = process.receive(subject, 1000)

  // Process message
  case msg {
    // Since we requested next message with `NoWait` option we get a
    // message with no `reply_to` subject when there is no new message
    // in the stream
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
```

## Installation

```sh
gleam add glats
```

and its documentation can be found at <https://hexdocs.pm/glats>.
