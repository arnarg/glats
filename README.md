# glats

[![Package Version](https://img.shields.io/hexpm/v/glats)](https://hex.pm/packages/glats)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/glats/)

A NATS client for Gleam. This wraps Elixir's client, [Gnat](https://hex.pm/packages/gnat).

## Publish

```gleam
import gleam/result
import glats
import glats/settings

pub fn main() {
  use conn <- result.then(
    settings.new("localhost", 4222)
    |> glats.connect,
  )

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
import glats/settings
import glats/message

pub fn main() {
  use conn <- result.then(
    settings.new("localhost", 4222)
    |> glats.connect,
  )

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
    message: message.Message(
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

## Installation

```sh
gleam add glats
```

and its documentation can be found at <https://hexdocs.pm/glats>.
