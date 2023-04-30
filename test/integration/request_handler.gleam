import gleam/io
import gleam/option.{None}
import gleam/result
import gleam/erlang/process
import glats
import glats/handler.{Reply, Request, Response}

pub fn main() {
  use conn <- result.then(glats.connect("localhost", 4222, []))

  let assert Ok(_actor) =
    handler.handle_request(conn, [], "do.ping", [], ping_pong_handler)

  let assert Ok(reply) = glats.request(conn, "do.ping", "Hello world", [], 1000)

  io.println("Got reply: " <> reply.body)

  process.sleep_forever()

  Ok(Nil)
}

pub fn ping_pong_handler(req: Request, state) {
  io.println("Got request: " <> req.body)

  // Reply with a message with the same headers and body.
  Reply(
    Response(
      headers: req.headers,
      reply_to: None,
      body: req.body <> " from glats!",
    ),
    state,
  )
}
