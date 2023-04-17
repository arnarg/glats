//// This module provides a subscription handler that can be used
//// to subscribe to NATS subjects and receive them in your own
//// handler.

import gleam/dynamic.{Dynamic}
import gleam/result
import gleam/map.{Map}
import gleam/option.{None, Some}
import gleam/otp/actor
import gleam/erlang/process.{Subject}
import glats/connection.{Command}
import glats/message.{Message}

/// Callback handler that should be provided to handle_subscription to process
/// the received messages from a subscription.
pub type MessageHandler =
  fn(Message, Subject(Command)) -> Result(Nil, String)

/// Callback handler that should be provided to handle_request to process the
/// the received request and return a response.
pub type RequestHandler =
  fn(Request, Subject(Command)) -> Result(Response, String)

/// Request includes headers and body from a request message.
pub type Request {
  Request(headers: Map(String, String), body: String)
}

/// Response includes headers and body for a response to a request.
pub type Response {
  Response(headers: Map(String, String), body: String)
}

// State for subscription handler.
type SubscriptionState {
  SubscriptionState(conn: Subject(Command), handler: MessageHandler)
}

// State for a request handler.
type RequestState {
  RequestState(conn: Subject(Command), handler: RequestHandler)
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
    loop: fn(msg: Message, state: SubscriptionState) {
      case state.handler(msg, state.conn) {
        Ok(_) -> actor.Continue(state)
        Error(_) -> actor.Stop(process.Abnormal("handler returned error!"))
      }
    },
  ))
}

/// Starts an actor that will handle received requests on a NATS subject and
/// handle responding to them with what's returned from the provided handler.
pub fn handle_request(
  conn: Subject(Command),
  subject: String,
  handler: RequestHandler,
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
        Ok(_) -> actor.Ready(RequestState(conn, handler), selector)
        Error(err) -> actor.Failed(err)
      }
    },
    init_timeout: 10_000,
    loop: request_handler_loop,
  ))
}

// Handler messages for request handler.
fn request_handler_loop(msg: Message, state: RequestState) {
  case msg.reply_to {
    Some(reply_to) ->
      case state.handler(Request(msg.headers, msg.body), state.conn) {
        Ok(response) ->
          case
            connection.publish_message(
              state.conn,
              Message(
                subject: reply_to,
                headers: response.headers,
                reply_to: None,
                body: response.body,
              ),
            )
          {
            Ok(Nil) -> actor.Continue(state)
            Error(err) -> actor.Stop(process.Abnormal(err))
          }
        Error(err) -> actor.Stop(process.Abnormal(err))
      }
    None -> actor.Continue(state)
  }
}

/// Converts the dynamic data returned from Gnat in elixir to a typed message
/// in gleam.
fn map_message(msg: Dynamic) {
  msg
  |> convert_msg
  |> result.unwrap(Message("error", map.new(), None, "body"))
}
