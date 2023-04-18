//// This module provides the most basic NATS client.

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
import glats/internal/decoder

pub type Connection =
  Subject(ConnectionMessage)

/// Errors that can be returned by the server.
pub type ServerError {
  Timeout
  NoResponders
  Unknown
}

//            //
// Connection //
//            //

/// Message sent to a NATS connection process.
pub opaque type ConnectionMessage {
  Subscribe(
    from: Subject(Result(Int, String)),
    subscriber: Subject(Message),
    subject: String,
    queue_group: Option(String),
  )
  Unsubscribe(from: Subject(Result(Nil, String)), sid: Int)
  Publish(from: Subject(Result(Nil, String)), message: Message)
  Request(
    from: Subject(Result(Message, ServerError)),
    subject: String,
    message: String,
  )
}

type SubscriptionActorMessage {
  GetSid(Subject(Int))
  ReceivedMessage(Message)
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
    subscriber: Subject(Message),
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
  List(#(Atom, String)),
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

/// Starts an actor that handles a connection to NATS using the provided
/// settings.
///
pub fn connect(settings: Settings) -> Result(Connection, StartError) {
  // Start actor for NATS connection handling.
  // This just starts Gnat's GenServer module linked to
  // the actor process and translates commands.
  actor.start_spec(actor.Spec(
    init: fn() {
      let subject = process.new_subject()
      let selector = process.new_selector()

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
    Publish(from, msg) -> handle_publish(from, msg, state)
    Request(from, subject, msg) -> handle_request(from, subject, msg, state)
    Subscribe(from, subscriber, subject, queue_group) ->
      handle_subscribe(from, subscriber, subject, queue_group, state)
    Unsubscribe(from, sid) -> handle_unsubscribe(from, sid, state)
    _ -> actor.Continue(state)
  }
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
fn handle_request(from, subject, message, state: ConnectionState) {
  case gnat_request(state.nats, subject, message, []) {
    Ok(msg) ->
      decoder.decode_msg(msg)
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
          fn(data) {
            data
            |> decoder.decode_msg
            |> result.map(ReceivedMessage)
            |> result.unwrap(DecodeError(data))
          },
        )

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

fn subscription_loop(
  message: SubscriptionActorMessage,
  state: SubscriptionActorState,
) {
  case message {
    GetSid(from) -> {
      actor.send(from, state.sid)
      actor.Continue(state)
    }
    ReceivedMessage(msg) -> {
      actor.send(state.subscriber, msg)
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
pub fn request(conn: Connection, subject: String, message: String) {
  process.call(conn, Request(_, subject, message), 5000)
}

//           //
// Subscribe //
//           //

/// Subscribes to a NATS subject that can be received on the
/// provided OTP subject.
///
pub fn subscribe(
  conn: Connection,
  subscriber: Subject(Message),
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
  subscriber: Subject(Message),
  subject: String,
  group: String,
) {
  process.call(conn, Subscribe(_, subscriber, subject, Some(group)), 5000)
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
