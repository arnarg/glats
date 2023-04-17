//// This module provides the most basic NATS client.

import gleam/option.{None, Some}
import gleam/map
import glats/connection.{Connection, Settings}
import glats/handler.{MessageHandler, RequestHandler}
import glats/message.{Message}

/// Starts an actor that handles a connection to NATS using the provided
/// settings.
pub fn connect(settings: Settings) {
  connection.start(settings)
}

/// Publishes a single message to NATS on a provided subject.
pub fn publish(conn: Connection, subject: String, message: String) {
  connection.publish(conn, subject, message)
}

/// Publishes a single message to NATS using the data from a provided `Message`
/// record.
pub fn publish_message(conn: Connection, message: Message) {
  connection.publish_message(conn, message)
}

/// Sends a request and listens for a response synchronously.
///
/// See [request-reply pattern docs.](https://docs.nats.io/nats-concepts/core-nats/reqreply)
pub fn request(conn: Connection, subject: String, message: String) {
  connection.request(conn, subject, message)
}

/// Start a subscription handler that will call the passed in handler
/// for every message received on the provided subject.
///
/// ```gleam
/// pub fn main() {
///   new_settings("localhost", 4222)
///   |> connect
///   |> handle_subscription("some.nats.subject", sub_handler)
///
///   process.sleep_forever()
/// }
///
/// pub fn sub_handler(message: Message, conn: Connection) {
///   io.debug(message)
///
///   Ok(Nil)
/// }
/// ```
pub fn handle_subscription(
  conn: Connection,
  subject: String,
  handler: MessageHandler,
) {
  handler.handle_subscription(conn, subject, handler)
}

/// Start a request handler that will call the passed in handler
/// for every message received on the provided subject and automatically
/// respond with the response returned by the handler.
///
/// ```gleam
/// pub fn main() {
///   new_settings("localhost", 4222)
///   |> connect
///   |> handle_request("some.nats.subject", req_handler)
///
///   process.sleep_forever()
/// }
///
/// pub fn req_handler(request: Request, conn: Connection) {
///   io.debug(request)
///
///   // Will respond with the same data as the request (ping pong).
///   Ok(Response(headers: request.headers, body: request.body))
/// }
/// ```
pub fn handle_request(
  conn: Connection,
  subject: String,
  handler: RequestHandler,
) {
  handler.handle_request(conn, subject, handler)
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
