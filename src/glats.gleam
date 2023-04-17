//// This module provides the most basic NATS client.
////
//// ## Example
////
//// ```gleam
//// pub fn main() {
////   // Connect to localhost:4222
////   let assert Ok(conn) =
////     glats.new_settings("localhost", 4222)
////     |> glats.connect
////
////   // Subscribe to subject "some.nats.subject"
////   let assert Ok(_) = conn
////     |> glats.handle_subscription("some.nats.subject", handle_message)
////
////   // Publish a single message to subject "some.nats.subject"
////   // with body "Hello worlds!"
////   let assert Ok(_) = conn
////     |> glats.publish("some.nats.subject", "Hello world!")
////
////   // Sleep for a second to receive the message in the handler
////   process.sleep(1000)
//// }
////
//// pub fn handle_message(message: glats.Message, conn: process.Subject(glats.Command)) {
////   io.debug(message)
////   // Prints `Message("some.nats.subject", //erl(#{}), None, "Hello world!")`
////
////   Ok(Nil)
//// }
//// ```

import gleam/option.{None, Some}
import gleam/map
import gleam/erlang/process.{Subject}
import glats/connection.{Command, Settings}
import glats/handler
import glats/message

pub type MessageHandler =
  handler.MessageHandler

pub type Message =
  message.Message

/// Starts an actor that handles a connection to NATS using the provided
/// settings.
pub fn connect(settings: Settings) {
  connection.start(settings)
}

/// Publishes a single message to NATS on a provided subject.
pub fn publish(conn: Subject(Command), subject: String, message: String) {
  connection.publish(conn, subject, message)
}

/// Publishes a single message to NATS using the data from a provided `Message`
/// record.
pub fn publish_message(conn: Subject(Command), message: Message) {
  connection.publish_message(conn, message)
}

/// Sends a request and listens for a response synchronously.
///
/// See [request-reply pattern docs.](https://docs.nats.io/nats-concepts/core-nats/reqreply)
pub fn request(conn: Subject(Command), subject: String, message: String) {
  connection.request(conn, subject, message)
}

/// Start a subscription handler that will call the passed in
/// for every message received on the provided subject.
pub fn handle_subscription(
  conn: Subject(Command),
  subject: String,
  handler: MessageHandler,
) {
  handler.handle_subscription(conn, subject, handler)
}

// Settings builders

/// Creates a settings with `host` and `port` set.
///
/// Use builder functions `with_*` to add additional options.
///
/// ```gleam
/// new_settings("localhost", 4222)
/// |> with_ca("/tmp/ca.crt")
/// ```
pub fn new_settings(host: String, port: Int) {
  Settings(host: Some(host), port: Some(port), tls: None, ssl_opts: None)
}

/// Returns settings with `localhost:4222`.
///
/// Use builder functions `with_*` to add additional options.
///
/// ```gleam
/// default_settings()
/// |> with_port(6222)
/// |> with_ca("/tmp/ca.crt")
/// ```
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
