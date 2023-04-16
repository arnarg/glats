//// This module provides the most basic NATS client.
////
//// ## Example
////
//// ```gleam
//// pub fn main() {
////   // Connect to localhost:4222
////   let assert Ok(nats) =
////     glats.new_settings("localhost", 4222)
////     |> glats.connect
////
////   // Subscribe to subject "some.nats.subject"
////   let assert Ok(_) = nats
////     |> glats.subscribe("some.nats.subject", handle_message)
////
////   // Publish a single message to subject "some.nats.subject"
////   // with body "Hello worlds!"
////   let assert Ok(_) = nats
////     |> glats.publish("some.nats.subject", "Hello world!")
////
////   // Sleep for a second to receive the message in the handler
////   process.sleep(1000)
//// }
////
//// pub fn handle_message(message: glats.Message, nats: process.Subject(glats.Command)) {
////   io.debug(message)
////   // Prints `Message("some.nats.subject", //erl(#{}), None, "Hello world!")`
////
////   Ok(Nil)
//// }
//// ```

import gleam/option.{None, Option, Some}
import gleam/map.{Map}
import gleam/dynamic.{Dynamic}
import gleam/list
import gleam/result
import gleam/erlang/atom.{Atom}
import gleam/erlang/process.{Pid, Subject}
import gleam/otp/actor

/// A single message that can be received from or sent to NATS.
pub type Message {
  Message(
    subject: String,
    headers: Map(String, String),
    reply_to: Option(String),
    body: String,
  )
}

/// Connection settings for NATS connection.
pub type Settings {
  Settings(
    host: Option(String),
    port: Option(Int),
    tls: Option(Bool),
    ssl_opts: Option(Map(String, String)),
  )
}

pub type ServerError {
  Timeout
  NoResponders
  Unknown
}

pub type Command {
  Subscribe(
    from: Subject(Result(String, String)),
    subscriber: Subject(Dynamic),
    subject: String,
  )
  Publish(from: Subject(Result(Nil, String)), message: Message)
  Request(
    from: Subject(Result(Message, ServerError)),
    subject: String,
    message: String,
  )
}

type ConnectionState {
  ConnectionState(nats: Pid)
}

type SubscriptionState {
  SubscriptionState(nats: Subject(Command), handler: MessageHandler)
}

// Externals from Gnat

pub external type Msg(data)

external fn gnat_start_link(
  settings: Map(String, Dynamic),
) -> actor.ErlangStartResult =
  "Elixir.Gnat" "start_link"

external fn gnat_pub(Pid, String, String, Dynamic) -> Result(Nil, String) =
  "Elixir.Gnat" "pub"

external fn gnat_request(Pid, String, String, Dynamic) -> Result(Dynamic, Atom) =
  "Elixir.Gnat" "request"

external fn gnat_sub(Pid, Pid, String, Dynamic) -> Result(String, String) =
  "Elixir.Gnat" "sub"

external fn convert_msg(Dynamic) -> Result(Message, String) =
  "Elixir.Glats" "convert_msg"

external fn convert_bare_msg(Dynamic) -> Result(Message, String) =
  "Elixir.Glats" "convert_bare_msg"

// Basic public API

/// Callback handler that should be provided to glats to process the received
/// messages from a subscription.
pub type MessageHandler =
  fn(Message, Subject(Command)) -> Result(Nil, String)

/// Starts an actor that handles a connection to NATS using the provided
/// settings.
pub fn connect(settings: Settings) {
  actor.start_spec(actor.Spec(
    init: fn() {
      let selector = process.new_selector()

      // Start linked process using Gnat's start_link
      case
        gnat_start_link(
          settings
          |> build_settings,
        )
      {
        Ok(pid) -> actor.Ready(ConnectionState(nats: pid), selector)
        Error(_) -> actor.Failed("starting connection failed")
      }
    },
    init_timeout: 5000,
    loop: connection_loop,
  ))
}

fn connection_loop(cmd: Command, state: ConnectionState) {
  case cmd {
    Publish(from, message) -> {
      case
        gnat_pub(state.nats, message.subject, message.body, dynamic.from([]))
      {
        Ok(Nil) -> process.send(from, Ok(Nil))
        _ -> process.send(from, Error("unknown publish error"))
      }
      actor.Continue(state)
    }
    Request(from, subject, message) -> {
      case gnat_request(state.nats, subject, message, dynamic.from([])) {
        Ok(msg) ->
          convert_bare_msg(msg)
          |> result.map_error(fn(_) { Unknown })
          |> process.send(from, _)
        Error(err) ->
          case
            err
            |> atom.to_string
          {
            "timeout" -> Error(Timeout)
            "no_responders" -> Error(NoResponders)
            _ -> Error(Unknown)
          }
          |> process.send(from, _)
      }
      actor.Continue(state)
    }
    Subscribe(from, receiver, subject) -> {
      gnat_sub(
        state.nats,
        receiver
        |> process.subject_owner,
        subject,
        dynamic.from([]),
      )
      |> process.send(from, _)

      actor.Continue(state)
    }
  }
}

/// Publishes a single message to NATS on a provided subject.
pub fn publish(nats: Subject(Command), subject: String, message: String) {
  publish_message(nats, new_message(subject, message))
}

/// Publishes a single message to NATS using the data from a provided `Message`
/// record.
pub fn publish_message(nats: Subject(Command), message: Message) {
  process.call(nats, Publish(_, message), 10_000)
}

/// Sends a request and listens for a response synchronously.
///
/// See [request-reply pattern docs.](https://docs.nats.io/nats-concepts/core-nats/reqreply)
pub fn request(nats: Subject(Command), subject: String, message: String) {
  process.call(nats, Request(_, subject, message), 10_000)
}

/// Subscribes to a NATS subject, providing a handler that will be called on
/// every message received for the subscription.
pub fn subscribe(
  nats: Subject(Command),
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
      case process.call(nats, Subscribe(_, receiver, subject), 10_000) {
        Ok(_) -> actor.Ready(SubscriptionState(nats, handler), selector)
        Error(err) -> actor.Failed(err)
      }
    },
    init_timeout: 10_000,
    loop: fn(msg: Message, state) {
      case state.handler(msg, state.nats) {
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

/// Constructs a new message.
pub fn new_message(subject: String, message: String) {
  Message(subject: subject, headers: map.new(), reply_to: None, body: message)
}

// Settings builders

/// Creates a settings with `host` and `port` set.
///
/// Use builder functions `with_*` to add additional options.
pub fn new_settings(host: String, port: Int) {
  Settings(host: Some(host), port: Some(port), tls: None, ssl_opts: None)
}

/// Returns settings with `localhost:4222`.
///
/// Use builder functions `with_*` to add additional options.
pub fn default_settings() {
  new_settings("localhost", 4222)
}

/// Sets the host for connection settings.
pub fn with_host(old: Settings, host: String) {
  Settings(..old, host: Some(host))
}

/// Sets the port for connection settings.
pub fn with_port(old: Settings, port: Int) {
  Settings(..old, port: Some(port))
}

/// Sets the CA file to use in connection settings.
pub fn with_ca(old: Settings, cafile: String) {
  Settings(
    ..old,
    tls: Some(True),
    ssl_opts: Some(
      old.ssl_opts
      |> option.unwrap(map.new())
      |> map.insert("cacertfile", cafile),
    ),
  )
}

/// Sets client certificates in connection settings.
pub fn with_client_cert(old: Settings, certfile: String, keyfile: String) {
  Settings(
    ..old,
    tls: Some(True),
    ssl_opts: Some(
      old.ssl_opts
      |> option.unwrap(map.new())
      |> map.insert("certfile", certfile)
      |> map.insert("keyfile", keyfile),
    ),
  )
}

/// Explicitly disables tls and resets ssl_opts for the connection settings.
pub fn with_no_tls(old: Settings) {
  Settings(..old, tls: Some(False), ssl_opts: None)
}

// Settings mapping helpers

fn build_settings(settings: Settings) {
  []
  |> take_host(settings)
  |> take_port(settings)
  |> take_tls(settings)
  |> take_ssl_opts(settings)
  |> map.from_list
}

fn take_host(old: List(#(String, Dynamic)), settings: Settings) {
  case settings.host {
    Some(host) ->
      old
      |> add_opt_to_list("host", host)
    None -> old
  }
}

fn take_port(old: List(#(String, Dynamic)), settings: Settings) {
  case settings.port {
    Some(port) ->
      old
      |> add_opt_to_list("port", port)
    None -> old
  }
}

fn take_tls(old: List(#(String, Dynamic)), settings: Settings) {
  case settings.tls {
    Some(tls) ->
      old
      |> add_opt_to_list("tls", tls)
    None -> old
  }
}

fn take_ssl_opts(old: List(#(String, Dynamic)), settings: Settings) {
  case settings.ssl_opts {
    Some(ssl_opts) ->
      old
      |> add_opt_to_list("ssl_opts", ssl_opts)
    None -> old
  }
}

fn add_opt_to_list(old: List(#(String, Dynamic)), key: String, value) {
  old
  |> list.append([#(key, dynamic.from(value))])
}
