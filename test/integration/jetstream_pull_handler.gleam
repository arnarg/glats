import gleam/io
import gleam/int
import gleam/string
import gleam/result
import gleam/function
import gleam/erlang/process
import glats.{Connection, Message}
import glats/jetstream/stream.{Retention, WorkQueuePolicy}
import glats/jetstream/consumer.{
  AckExplicit, AckPolicy, BindStream, Description, With,
}
import glats/jetstream/handler.{Ack}

pub fn main() {
  use conn <- result.then(glats.connect("localhost", 4222, []))

  // Create a stream
  let assert Ok(stream) =
    stream.create(conn, "wqstream", ["ticket.>"], [Retention(WorkQueuePolicy)])

  // Run pull handler
  let assert Ok(_actor) =
    handler.handle_pull_consumer(
      conn,
      0,
      "ticket.*",
      100,
      pull_handler,
      [
        // Bind to stream created above
        BindStream(stream.config.name),
        // Set description for the ephemeral consumer
        With(Description("An ephemeral consumer for subscription")),
        // Set ack policy for the consumer
        With(AckPolicy(AckExplicit)),
      ],
    )

  // Run a loop that publishes a message every 100ms
  publish_loop(conn, 0)

  Ok(Nil)
}

// Publishes a new message every 100ms
fn publish_loop(conn: Connection, counter: Int) {
  let assert Ok(Nil) =
    glats.publish(conn, "ticket." <> int.to_string(counter), "ticket body", [])

  process.sleep(100)

  publish_loop(conn, counter + 1)
}

// Handler function for the pull consumer handler
pub fn pull_handler(message: Message, state) {
  // Increment state counter, print message and instruct
  // pull handler to ack the message.
  state + 1
  |> function.tap(print_message(_, message.topic, message.body))
  |> Ack
}

fn print_message(num: Int, topic: String, body: String) {
  "message " <> int.to_string(num) <> " (" <> topic <> "): " <> body
  |> io.println
}
