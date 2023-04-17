import gleam/map.{Map}

/// A single response from a NATS subject used in a response handler.
pub type Response {
  Response(headers: Map(String, String), body: String)
}

/// Constructs a new response.
pub fn new(body: String) {
  Response(headers: map.new(), body: body)
}

/// Sets a header in a response.
pub fn set_header(response: Response, key: String, value: String) {
  Response(
    ..response,
    headers: response.headers
    |> map.insert(key, value),
  )
}
