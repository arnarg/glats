import gleam/io
import gleam/option.{None}
import gleam/result
import gleam/erlang/process
import glats
import glats/handler.{Reply, Response}

pub fn main() {
  use conn <- result.then(glats.connect("localhost", 4222, []))

  // Start a request handler actor that will call `ping_pong_handler`
  // for every request received from NATS topic "do.ping".
  let assert Ok(_actor) =
    handler.handle_request(conn, [], "do.ping", [], ping_pong_handler)

  process.sleep_forever()

  Ok(Nil)
}

pub fn ping_pong_handler(req: handler.Request, state) {
  // Got message: Hello
  io.println("Got message: " <> req.body)

  // Reply with a message with the same headers and append to body.
  Reply(
    Response(
      headers: req.headers,
      reply_to: None,
      body: req.body <> " from glats!",
    ),
    state,
  )
}
