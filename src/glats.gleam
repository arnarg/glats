import gleam/io
import gleam/result
import gleam/dynamic.{Dynamic}
import gleam/map.{Map}
import gleam/option.{None, Option, Some}
import gleam/list
import gleam/otp/actor.{StartError}
import gleam/erlang/atom.{Atom}
import gleam/erlang/process.{Pid, Subject}
import glats/settings.{Settings}
import glats/message.{Message}
import glats/protocol.{ServerInfo}
import glats/internal/decoder

pub type Connection =
  Subject(ConnectionMessage)

/// Errors that can be returned by the server.
///
pub type ConnectionError {
  Timeout
  NoResponders
  Unexpected
}

//            //
// Connection //
//            //

/// Message sent to a NATS connection process.
pub opaque type ConnectionMessage {
  Subscribe(
    from: Subject(Result(Int, String)),
    subscriber: Subject(SubscriptionMessage),
    subject: String,
    queue_group: Option(String),
  )
  Unsubscribe(from: Subject(Result(Nil, String)), sid: Int)
  Publish(from: Subject(Result(Nil, String)), message: Message)
  Request(
    from: Subject(fn() -> Result(Message, ConnectionError)),
    subject: String,
    message: String,
    timeout: Int,
  )
  GetServerInfo(from: Subject(Result(ServerInfo, ConnectionError)))
  GetActiveSubscriptions(from: Subject(Result(Int, ConnectionError)))
  Exited(process.ExitMessage)
}

/// Message received by a subscribing subject.
///
pub type SubscriptionMessage {
  ReceivedMessage(conn: Connection, sid: Int, message: Message)
}

// This message type is for the interim translating actor that receives
// messages from Gnat, decodes them into Gleam native `Message` and
// passes that onto the actual subscriber.
type SubscriptionActorMessage {
  GetSid(Subject(Int))
  IncomingMessage(Int, Message)
  DecodeError(Dynamic)
  SubscriberExited
}

type ConnectionState {
  ConnectionState(nats: Pid, self: Subject(ConnectionMessage))
}

type SubscriptionActorState {
  SubscriptionActorState(
    conn: Connection,
    sid: Int,
    subscriber: Subject(SubscriptionMessage),
  )
}

// Gnat's GenServer start_link.
external fn gnat_start_link(
  settings: Map(String, Dynamic),
) -> actor.ErlangStartResult =
  "Elixir.Gnat" "start_link"

// Gnat's publish function.
external fn gnat_pub(Pid, String, String, List(#(Atom, String))) -> Atom =
  "Elixir.Gnat" "pub"

// Gnat's request function.
external fn gnat_request(
  Pid,
  String,
  String,
  List(#(Atom, Dynamic)),
) -> Result(Dynamic, Atom) =
  "Elixir.Gnat" "request"

// Gnat's subscribe function.
external fn gnat_sub(
  Pid,
  Pid,
  String,
  List(#(Atom, String)),
) -> Result(Int, String) =
  "Elixir.Gnat" "sub"

// Gnat's unsubscribe function.
external fn gnat_unsub(Pid, Int, List(#(Atom, String))) -> Atom =
  "Elixir.Gnat" "unsub"

// Gnat's server_info function.
external fn gnat_server_info(Pid) -> Dynamic =
  "Elixir.Gnat" "server_info"

// Gnat's active_subscriptions function.
external fn gnat_active_subscriptions(Pid) -> Result(Int, Dynamic) =
  "Elixir.Gnat" "active_subscriptions"

/// Starts an actor that handles a connection to NATS using the provided
/// settings.
///
pub fn connect(settings: Settings) -> Result(Connection, StartError) {
  // Start actor for NATS connection handling.
  // This just starts Gnat's GenServer module linked to
  // the actor process and translates commands.
  actor.start_spec(actor.Spec(
    init: fn() {
      process.trap_exits(True)

      let subject = process.new_subject()
      let selector =
        process.new_selector()
        |> process.selecting_trapped_exits(Exited)
        |> process.selecting(subject, fn(msg) { msg })

      // Start linked process using Gnat's start_link
      case
        gnat_start_link(
          settings
          |> build_settings,
        )
      {
        Ok(pid) ->
          actor.Ready(ConnectionState(nats: pid, self: subject), selector)
        Error(_) -> actor.Failed("starting connection failed")
      }
    },
    init_timeout: 5000,
    loop: handle_command,
  ))
}

// Runs for every command received.
//
fn handle_command(message: ConnectionMessage, state: ConnectionState) {
  case message {
    Exited(em) -> actor.Stop(em.reason)
    Publish(from, msg) -> handle_publish(from, msg, state)
    Request(from, subject, msg, timeout) ->
      handle_request(from, subject, msg, timeout, state)
    Subscribe(from, subscriber, subject, queue_group) ->
      handle_subscribe(from, subscriber, subject, queue_group, state)
    Unsubscribe(from, sid) -> handle_unsubscribe(from, sid, state)
    GetServerInfo(from) -> handle_server_info(from, state)
    GetActiveSubscriptions(from) -> handle_active_subscriptions(from, state)
    _ -> actor.Continue(state)
  }
}

// Handles a single server info command.
//
fn handle_server_info(from, state: ConnectionState) {
  gnat_server_info(state.nats)
  |> decoder.decode_server_info
  |> result.map_error(fn(_) { Unexpected })
  |> process.send(from, _)

  actor.Continue(state)
}

// Handles a single active subscriptions command.
//
fn handle_active_subscriptions(from, state: ConnectionState) {
  gnat_active_subscriptions(state.nats)
  |> result.map_error(fn(_) { Unexpected })
  |> process.send(from, _)

  actor.Continue(state)
}

// Handles a single publish command.
//
fn handle_publish(from, message: Message, state: ConnectionState) {
  case
    gnat_pub(state.nats, message.subject, message.body, [])
    |> atom.to_string
  {
    "ok" -> process.send(from, Ok(Nil))
    _ -> process.send(from, Error("unknown publish error"))
  }
  actor.Continue(state)
}

// Handles a single request command.
//
fn handle_request(from, subject, message, timeout, state: ConnectionState) {
  let opts = [
    #(atom.create_from_string("receive_timeout"), dynamic.from(timeout)),
  ]

  // In order to not block the connection actor we return a function
  // that will make the request.
  let req_func = fn() {
    case gnat_request(state.nats, subject, message, opts) {
      Ok(msg) ->
        decoder.decode_msg(msg)
        |> result.map_error(fn(_) { Unexpected })
      Error(err) ->
        case atom.to_string(err) {
          "timeout" -> Error(Timeout)
          "no_responders" -> Error(NoResponders)
          _ -> Error(Unexpected)
        }
    }
  }

  process.send(from, req_func)

  actor.Continue(state)
}

// Handles a single unsubscribe command.
//
fn handle_unsubscribe(from, sid, state: ConnectionState) {
  case
    gnat_unsub(state.nats, sid, [])
    |> atom.to_string
  {
    "ok" -> process.send(from, Ok(Nil))
    _ -> process.send(from, Error("unknown unsubscribe error"))
  }
  actor.Continue(state)
}

// Handles a single subscribe command.
//
fn handle_subscribe(
  from,
  subscriber,
  subject: String,
  queue_group: Option(String),
  state: ConnectionState,
) {
  case start_subscription_actor(subscriber, subject, queue_group, state) {
    Ok(actor) ->
      case process.try_call(actor, GetSid, 1000) {
        Ok(sid) -> process.send(from, Ok(sid))
        Error(_) -> process.send(from, Error("subscribe failed"))
      }
    Error(_) -> process.send(from, Error("subscribe failed"))
  }

  actor.Continue(state)
}

//                    //
// Subscription Actor //
//                    //

// Since Gnat will send messages from Elixir we need to translate it
// to a type in Gleam _before_ passing it to the user.
// This is done by starting a new actor that will receive the messages
// from Gnat, decode them into `Message` and then send it to the actual
// subscribing subject.
//
fn start_subscription_actor(
  subscriber,
  subject,
  queue_group,
  state: ConnectionState,
) {
  actor.start_spec(actor.Spec(
    init: fn() {
      // Monitor subscriber process.
      let monitor =
        process.monitor_process(
          subscriber
          |> process.subject_owner,
        )

      let selector =
        process.new_selector()
        |> process.selecting_process_down(monitor, fn(_) { SubscriberExited })
        |> process.selecting_record2(
          atom.create_from_string("msg"),
          map_gnat_message,
        )

      // If a queue group was provided we should pass the option
      // to gnat_sup.
      let opts = case queue_group {
        Some(group) -> [#(atom.create_from_string("queue_group"), group)]
        None -> []
      }

      case gnat_sub(state.nats, process.self(), subject, opts) {
        Ok(sid) ->
          actor.Ready(
            SubscriptionActorState(state.self, sid, subscriber),
            selector,
          )
        Error(err) -> actor.Failed(err)
      }
    },
    init_timeout: 5000,
    loop: subscription_loop,
  ))
}

fn map_gnat_message(data: Dynamic) -> SubscriptionActorMessage {
  let sid_ =
    data
    |> dynamic.field(atom.create_from_string("sid"), dynamic.int)

  case sid_ {
    Ok(sid) ->
      data
      |> decoder.decode_msg
      |> result.map(IncomingMessage(sid, _))
      |> result.unwrap(DecodeError(data))
    Error(_) -> DecodeError(data)
  }
}

fn subscription_loop(
  message: SubscriptionActorMessage,
  state: SubscriptionActorState,
) {
  case message {
    GetSid(from) -> {
      actor.send(from, state.sid)
      actor.Continue(state)
    }
    IncomingMessage(sid, msg) -> {
      actor.send(state.subscriber, ReceivedMessage(state.conn, sid, msg))
      actor.Continue(state)
    }
    DecodeError(data) -> {
      io.debug(data)
      actor.Continue(state)
    }
    SubscriberExited -> {
      io.println("subscriber exited")
      // TODO: handle properly
      unsubscribe(state.conn, state.sid)
      actor.Stop(process.Normal)
    }
  }
}

//         //
// Publish //
//         //

/// Publishes a single message to NATS on a provided subject.
///
pub fn publish(conn: Connection, subject: String, message: String) {
  publish_message(conn, Message(subject, map.new(), None, message))
}

/// Publishes a single message to NATS using the data from a provided `Message`
/// record.
///
pub fn publish_message(conn: Connection, message: Message) {
  process.call(conn, Publish(_, message), 5000)
}

/// Sends a request and listens for a response synchronously.
///
/// See [request-reply pattern docs.](https://docs.nats.io/nats-concepts/core-nats/reqreply)
///
/// To handle a request from NATS see `handler.handle_request`.
///
pub fn request(conn: Connection, subject: String, message: String, timeout: Int) {
  // Because Gnat's request function is blocking the connection actor will return
  // a function with all the data set in a clojure that calls the function.
  // This is done in order to not block the entire connection actor when waiting
  // for a response.
  let make_request =
    process.call(conn, Request(_, subject, message, timeout), 1000)

  // Call the request function returned by the connection actor.
  make_request()
}

//           //
// Subscribe //
//           //

/// Subscribes to a NATS subject that can be received on the
/// provided OTP subject.
///
pub fn subscribe(
  conn: Connection,
  subscriber: Subject(SubscriptionMessage),
  subject: String,
) {
  process.call(conn, Subscribe(_, subscriber, subject, None), 5000)
}

/// Unsubscribe from a subscription by providing the subscription ID.
///
pub fn unsubscribe(conn: Connection, sid: Int) {
  process.call(conn, Unsubscribe(_, sid), 5000)
}

/// Subscribes to a NATS subject as part of a queue group.
/// Messages can be received on the provided OTP subject.
///
/// See [Queue Groups docs.](https://docs.nats.io/nats-concepts/core-nats/queue)
///
pub fn queue_subscribe(
  conn: Connection,
  subscriber: Subject(SubscriptionMessage),
  subject: String,
  group: String,
) {
  process.call(conn, Subscribe(_, subscriber, subject, Some(group)), 5000)
}

//             //
// Server Info //
//             //

/// Returns server info provided by the connected NATS server.
///
pub fn server_info(conn: Connection) {
  process.call(conn, GetServerInfo, 5000)
}

//                      //
// Active Subscriptions //
//                      //

/// Returns the number of active subscriptions for the connection.
///
pub fn active_subscriptions(conn: Connection) {
  process.call(conn, GetActiveSubscriptions, 5000)
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
