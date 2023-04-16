//// This module provides a subscription handler that can be used
//// to subscribe to NATS subjects and receive them in your own
//// handler.

import gleam/dynamic.{Dynamic}
import gleam/result
import gleam/map
import gleam/option.{None}
import gleam/otp/actor
import gleam/erlang/process.{Subject}
import glats/connection.{Command}
import glats/message.{Message}

/// Callback handler that should be provided to glats to process the received
/// messages from a subscription.
pub type MessageHandler =
  fn(Message, Subject(Command)) -> Result(Nil, String)

// State for subscription handler.
type State {
  SubscriptionState(conn: Subject(Command), handler: MessageHandler)
}

// Externals from Gnat

external fn convert_msg(Dynamic) -> Result(Message, String) =
  "Elixir.Glats" "convert_msg"

/// Starts an actor that will handle receiving messages from a NATS subject and
/// call your handler.
pub fn handle_subscription(
  conn: Subject(Command),
  subject: String,
  handler: MessageHandler,
) {
  // Start a new actor that will subscribe to NATS messages.
  actor.start_spec(actor.Spec(
    init: fn() {
      let receiver = process.new_subject()
      let selector =
        process.new_selector()
        |> process.selecting_anything(map_message)

      // Send a subscription request to connection process.
      case connection.subscribe(conn, receiver, subject) {
        Ok(_) -> actor.Ready(SubscriptionState(conn, handler), selector)
        Error(err) -> actor.Failed(err)
      }
    },
    init_timeout: 10_000,
    loop: fn(msg: Message, state) {
      case state.handler(msg, state.conn) {
        Ok(_) -> actor.Continue(state)
        Error(_) -> actor.Stop(process.Abnormal("handler returned error!"))
      }
    },
  ))
}

/// Converts the dynamic data returned from Gnat in elixir to a typed message
/// in gleam.
fn map_message(msg: Dynamic) {
  msg
  |> convert_msg
  |> result.unwrap(Message("error", map.new(), None, "body"))
}
