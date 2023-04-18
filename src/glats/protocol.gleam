import gleam/option.{Option}

/// Server info returned by the NATS server.
pub type ServerInfo {
  ServerInfo(
    server_id: String,
    server_name: String,
    version: String,
    go: String,
    host: String,
    port: Int,
    headers: Bool,
    max_payload: Int,
    proto: Int,
    jetstream: Bool,
    auth_required: Option(Bool),
  )
}
