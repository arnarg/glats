//// A convenience handler that will handle a consumer subscription for you.
//// For every message it receives it will call the provided `SubscriptionHandler(a)`
//// function and take action depending on its return value.
////
//// It will also keep the state for you of type `a`.
////
//// ## Pull consumer example
////
//// ```gleam
//// import gleam/io
//// import gleam/int
//// import gleam/string
//// import gleam/result
//// import gleam/function
//// import gleam/erlang/process
//// import glats.{Connection, Message}
//// import glats/jetstream/stream.{Retention, WorkQueuePolicy}
//// import glats/jetstream/consumer.{
////   AckExplicit, AckPolicy, BindStream, Description, With,
//// }
//// import glats/jetstream/handler.{Ack}
//// 
//// pub fn main() {
////   use conn <- result.then(glats.connect("localhost", 4222, []))
//// 
////   // Create a stream
////   let assert Ok(stream) =
////     stream.create(conn, "wqstream", ["ticket.>"], [Retention(WorkQueuePolicy)])
//// 
////   // Run pull handler
////   let assert Ok(_actor) =
////     handler.handle_pull_consumer(
////       conn,
////       0,            // Initial state
////       "ticket.*",   // Topic
////       100,          // Batch size
////       pull_handler, // Handler function
////       [
////         // Bind to stream created above
////         BindStream(stream.config.name),
////         // Set description for the ephemeral consumer
////         With(Description("An ephemeral consumer for subscription")),
////         // Set ack policy for the consumer
////         With(AckPolicy(AckExplicit)),
////       ],
////     )
//// 
////   // Run a loop that publishes a message every 100ms
////   publish_loop(conn, 0)
//// 
////   Ok(Nil)
//// }
//// 
//// // Publishes a new message every 100ms
//// fn publish_loop(conn: Connection, counter: Int) {
////   let assert Ok(Nil) =
////     glats.publish(
////       conn,
////       "ticket." <> int.to_string(counter),
////       "ticket body",
////       [],
////     )
//// 
////   process.sleep(100)
//// 
////   publish_loop(conn, counter + 1)
//// }
//// 
//// // Handler function for the pull consumer handler
//// pub fn pull_handler(message: Message, state) {
////   // Increment state counter, print message and instruct
////   // pull handler to ack the message.
////   state + 1
////   |> function.tap(print_message(_, message.topic, message.body))
////   |> Ack
//// }
////
//// fn print_message(num: Int, topic: String, body: String) {
////   "message " <> int.to_string(num) <> " (" <> topic <> "): " <> body
////   |> io.println
//// }
//// ```
////
//// Will output:
////
//// ```sh
//// message 1 (ticket.0): ticket body
//// message 2 (ticket.1): ticket body
//// message 3 (ticket.2): ticket body
//// message 4 (ticket.3): ticket body
//// message 5 (ticket.4): ticket body
//// ...
//// ```

import gleam/string
import gleam/option.{None, Some}
import gleam/function.{identity}
import gleam/otp/actor
import gleam/erlang/process.{Abnormal}
import glats
import glats/jetstream
import glats/jetstream/consumer

const expires = 10_000

/// Used to instruct the consumer handler what to do
/// with the processed message.
/// Also used to return new state.
///
pub type Outcome(a) {
  /// Acknowledge message and save state.
  Ack(a)
  /// Negatively acknowledge message and save state.
  Nack(a)
  /// Terminate message and save state.
  Term(a)
  /// Do nothing with message and save state.
  NoReply(a)
}

/// The handler func that should be passed to the consumer handler.
///
pub type SubscriptionHandler(a) =
  fn(glats.Message, a) -> Outcome(a)

type PullHandlerState(a) {
  PullHandlerState(
    conn: glats.Connection,
    sub: consumer.Subscription,
    batch_size: Int,
    pending: Int,
    handler: SubscriptionHandler(a),
    inner_state: a,
  )
}

/// Start a pull consumer handler actor.
///
pub fn handle_pull_consumer(
  conn: glats.Connection,
  initial_state: a,
  topic: String,
  batch_size: Int,
  handler: SubscriptionHandler(a),
  opts: List(consumer.SubscriptionOption),
) {
  actor.start_spec(actor.Spec(
    init: fn() {
      let subject = process.new_subject()
      let selector =
        process.new_selector()
        |> process.selecting(subject, identity)

      case consumer.subscribe(conn, subject, topic, opts) {
        Ok(sub) -> {
          // Request initial batch
          case
            consumer.request_batch(sub, [
              consumer.Batch(batch_size),
              consumer.Expires(expires * 1_000_000),
            ])
          {
            Ok(Nil) ->
              actor.Ready(
                PullHandlerState(
                  conn,
                  sub,
                  batch_size,
                  batch_size,
                  handler,
                  initial_state,
                ),
                selector,
              )
            Error(err) -> actor.Failed(string.inspect(err))
          }
        }

        Error(err) -> actor.Failed(string.inspect(err))
      }
    },
    init_timeout: 5000,
    loop: pull_loop,
  ))
}

fn request_more(state: PullHandlerState(a)) {
  case
    consumer.request_batch(state.sub, [
      consumer.Batch(state.batch_size),
      consumer.Expires(expires * 1_000_000),
    ])
  {
    Ok(Nil) ->
      actor.Continue(PullHandlerState(..state, pending: state.batch_size), None)
    Error(err) -> actor.Stop(Abnormal(string.inspect(err)))
  }
}

fn pull_loop(message: glats.SubscriptionMessage, state: PullHandlerState(a)) {
  case message {
    // Request expired.
    glats.ReceivedMessage(status: Some(408), ..) -> request_more(state)
    // No new messages
    glats.ReceivedMessage(status: Some(404), ..) -> request_more(state)
    // New message
    glats.ReceivedMessage(message: msg, ..) -> handle_pull_message(msg, state)
  }
}

fn handle_pull_message(message: glats.Message, state: PullHandlerState(a)) {
  let inner = case state.handler(message, state.inner_state) {
    Ack(inner) -> {
      jetstream.ack(state.conn, message)
      inner
    }
    Nack(inner) -> {
      jetstream.nack(state.conn, message)
      inner
    }
    Term(inner) -> {
      jetstream.term(state.conn, message)
      inner
    }
    NoReply(inner) -> inner
  }

  // Keep track of remaining messages in the request
  case state.pending <= 1 {
    // Request more
    True -> request_more(PullHandlerState(..state, inner_state: inner))
    // Decrement the counter
    False ->
      actor.Continue(
        PullHandlerState(
          ..state,
          pending: state.pending
          - 1,
          inner_state: inner,
        ),
        None,
      )
  }
}
