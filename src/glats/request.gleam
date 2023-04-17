import gleam/map.{Map}

/// A single request from a NATS subject used in a request handler.
pub type Request {
  Request(headers: Map(String, String), body: String)
}

/// Constructs a new request.
pub fn new(body: String) {
  Request(headers: map.new(), body: body)
}

/// Sets a header in a request.
pub fn set_header(request: Request, key: String, value: String) {
  Request(
    ..request,
    headers: request.headers
    |> map.insert(key, value),
  )
}
