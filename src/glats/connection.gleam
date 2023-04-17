import gleam/map.{Map}
import gleam/list
import gleam/dynamic.{Dynamic}
import gleam/option.{None, Option, Some}
import gleam/result
import gleam/otp/actor
import gleam/erlang/atom.{Atom}
import gleam/erlang/process.{Pid, Subject}
import glats/message.{Message, new_message}

/// Connection settings for NATS connection.
pub type Settings {
  Settings(
    host: Option(String),
    port: Option(Int),
    tls: Option(Bool),
    ssl_opts: Option(Map(String, String)),
  )
}

/// Command to send to the connection actor.
pub opaque type Command {
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

/// Error returned by a NATS server.
pub type ServerError {
  Timeout
  NoResponders
  Unknown
}

/// State kept by the connection actor.
type State {
  ConnectionState(nats: Pid)
}

// External from Gnat

external fn gnat_start_link(
  settings: Map(String, Dynamic),
) -> actor.ErlangStartResult =
  "Elixir.Gnat" "start_link"

external fn gnat_pub(Pid, String, String, Dynamic) -> Atom =
  "Elixir.Gnat" "pub"

external fn gnat_request(Pid, String, String, Dynamic) -> Result(Dynamic, Atom) =
  "Elixir.Gnat" "request"

external fn gnat_sub(Pid, Pid, String, Dynamic) -> Result(String, String) =
  "Elixir.Gnat" "sub"

external fn convert_bare_msg(Dynamic) -> Result(Message, String) =
  "Elixir.Glats" "convert_bare_msg"

/// Start an actor that handles a connection to NATS using
/// the provided settings.
pub fn start(settings: Settings) {
  // Start actor for NATS connection handling.
  // This just starts Gnat's GenServer module linked to
  // the actor process and translates commands.
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
    loop: handle_command,
  ))
}

/// Publishes a single message to NATS on a provided subject.
pub fn publish(conn: Subject(Command), subject: String, message: String) {
  publish_message(conn, new_message(subject, message))
}

/// Publishes a single message to NATS using the data from a provided `Message`
/// record.
pub fn publish_message(conn: Subject(Command), message: Message) {
  process.call(conn, Publish(_, message), 10_000)
}

/// Sends a request and listens for a response synchronously.
pub fn request(conn: Subject(Command), subject: String, message: String) {
  process.call(conn, Request(_, subject, message), 10_000)
}

// Subscribes to a NATS subject.
pub fn subscribe(
  conn: Subject(Command),
  receiver: Subject(Dynamic),
  subject: String,
) {
  process.call(conn, Subscribe(_, receiver, subject), 10_000)
}

// Runs for every command received.
fn handle_command(cmd: Command, state: State) {
  case cmd {
    Publish(from, message) -> handle_publish(from, message, state)
    Request(from, subject, message) ->
      handle_request(from, subject, message, state)
    Subscribe(from, receiver, subject) ->
      handle_subscribe(from, receiver, subject, state)
    _ -> actor.Continue(state)
  }
}

// Handles a single publish command.
fn handle_publish(from, message: Message, state: State) {
  case
    gnat_pub(state.nats, message.subject, message.body, dynamic.from([]))
    |> atom.to_string
  {
    "ok" -> process.send(from, Ok(Nil))
    _ -> process.send(from, Error("unknown publish error"))
  }
  actor.Continue(state)
}

// Handles a single request command.
fn handle_request(from, subject, message, state: State) {
  case gnat_request(state.nats, subject, message, dynamic.from([])) {
    Ok(msg) ->
      convert_bare_msg(msg)
      |> result.map_error(fn(_) { Unknown })
      |> process.send(from, _)
    Error(err) ->
      case atom.to_string(err) {
        "timeout" -> Error(Timeout)
        "no_responders" -> Error(NoResponders)
        _ -> Error(Unknown)
      }
      |> process.send(from, _)
  }
  actor.Continue(state)
}

// Handles a single subscribe command.
fn handle_subscribe(from, receiver, subject, state: State) {
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

// Settings mapping helpers to create a map out of the settings
// type, which is expected by Gnat's start_link.

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
