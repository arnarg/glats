import gleam/option.{None, Option, Some}
import gleam/map.{Map}

/// Connection settings for NATS connection.
pub type Settings {
  Settings(
    host: Option(String),
    port: Option(Int),
    tls: Option(Bool),
    ssl_opts: Option(Map(String, String)),
  )
}

/// Creates a settings with `host` and `port` set.
///
/// Use builder functions `set_*` to add additional options.
///
/// ```gleam
/// new("localhost", 4222)
/// |> set_ca("/tmp/ca.crt")
/// ```
pub fn new(host: String, port: Int) {
  Settings(host: Some(host), port: Some(port), tls: None, ssl_opts: None)
}

/// Returns settings with `localhost:4222`.
///
/// Use builder functions `set_*` to add additional options.
///
/// ```gleam
/// defaults()
/// |> set_port(6222)
/// |> set_ca("/tmp/ca.crt")
/// ```
pub fn defaults() {
  new("localhost", 4222)
}

/// Sets the host for connection settings.
pub fn set_host(settings: Settings, host: String) {
  Settings(..settings, host: Some(host))
}

/// Sets the port for connection settings.
pub fn set_port(settings: Settings, port: Int) {
  Settings(..settings, port: Some(port))
}

/// Sets the CA file to use in connection settings.
pub fn set_ca(settings: Settings, cafile: String) {
  Settings(
    ..settings,
    tls: Some(True),
    ssl_opts: Some(
      settings.ssl_opts
      |> option.unwrap(map.new())
      |> map.insert("cacertfile", cafile),
    ),
  )
}

/// Sets client certificates in connection settings.
pub fn set_client_cert(settings: Settings, certfile: String, keyfile: String) {
  Settings(
    ..settings,
    tls: Some(True),
    ssl_opts: Some(
      settings.ssl_opts
      |> option.unwrap(map.new())
      |> map.insert("certfile", certfile)
      |> map.insert("keyfile", keyfile),
    ),
  )
}

/// Explicitly disables tls and resets ssl_opts for the connection settings.
pub fn set_no_tls(settings: Settings) {
  Settings(..settings, tls: Some(False), ssl_opts: None)
}
