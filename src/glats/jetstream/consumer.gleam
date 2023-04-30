import gleam/string
import gleam/map.{Map}
import gleam/dynamic.{Dynamic}
import gleam/option.{None, Option, Some}
import gleam/list
import gleam/result
import gleam/json.{Json}
import gleam/erlang/process.{Subject}
import glats.{Connection, Message, SubscriptionMessage}
import glats/jetstream.{JetstreamError}
import glats/jetstream/stream
import glats/internal/js
import glats/internal/util

const consumer_prefix = "$JS.API.CONSUMER"

/// Available options to set during subscribe to a stream subject.
///
pub type SubscriptionOption {
  /// Used to bind to an existing stream and consumer while subscribing.
  Bind(String, String)
  /// Used to bind to an existing stream.
  BindStream(String)
  /// When not binding to an existing consumer this can be used to add
  /// consumer options for the consumer that will be created automatically.
  With(ConsumerOption)
}

/// An active subscription to a consumer.
///
pub opaque type Subscription {
  PullSubscription(
    conn: Connection,
    sid: Int,
    stream: String,
    consumer: String,
    inbox: String,
  )
  PushSubscription(conn: Connection, sid: Int)
}

/// Available options to set during consumer creation and update.
///
pub type ConsumerOption {
  /// If set, clients can have subscriptions bind to the consumer and
  /// resume until the consumer is explicitly deleted. A durable name
  /// cannot contain whitespace, `.`, `*`, `>`, path separators
  /// (forward or backwards slash), and non-printable characters.
  DurableName(String)
  /// A description of the consumer. This can be particularly useful
  /// for ephemeral consumers to indicate their purpose since the durable
  /// name cannot be provided.
  Description(String)
  /// An overlapping subject with the subjects bound to the stream
  /// which will filter the set of messages received by the consumer.
  FilterSubject(String)
  /// The requirement of client acknowledgements, either `AckExplicit`,
  /// `AckNone`, or `AckAll`.
  AckPolicy(AckPolicy)
  /// The duration that the server will wait for an ack for any individual
  /// message once it has been delivered to a consumer. If an ack is not
  /// received in time, the message will be redelivered.
  AckWait(Int)
  /// The point in the stream to receive messages from, either `DeliverAll`,
  /// `DeliverLast`, `DeliverNew`, `DeliverByStartSequence`, `DeliverByStartTime`,
  /// or `DeliverLastPerSubject`.
  DeliverPolicy(DeliverPolicy)
  /// Duration that instructs the server to cleanup consumers that are inactive
  /// for that long. Prior to 2.9, this only applied to ephemeral consumers.
  InactiveThreshold(Int)
  /// Defines the maximum number of messages, without an acknowledgement, that
  /// can be outstanding. Once this limit is reached message delivery will be
  /// suspended. This limit applies across all of the consumer's bound
  /// subscriptions. A value of -1 means there can be any number of pending acks
  /// (i.e. no flow control). This does not apply when the `AckNone` policy is used.
  MaxAckPending(Int)
  /// The maximum number of times a specific message delivery will be attempted.
  /// Applies to any message that is re-sent due to ack policy (i.e. due to a
  /// negative ack, or no ack sent by the client).
  MaxDeliver(Int)
  /// If the policy is `ReplayOriginal`, the messages in the stream will be pushed
  /// to the client at the same rate that they were originally received, simulating
  /// the original timing of messages. If the policy is `ReplayInstant` (the default),
  /// the messages will be pushed to the client as fast as possible while adhering
  /// to the Ack Policy, Max Ack Pending and the client's ability to consume those
  /// messages.
  ReplayPolicy(ReplayPolicy)
  /// Sets the number of replicas for the consumer's state. By default, when the
  /// value is set to zero, consumers inherit the number of replicas from the stream.
  NumReplicas(Int)
  /// Sets the percentage of acknowledgements that should be sampled for observability,
  /// 0-100 This value is a string and for example allows both `30` and `30%` as valid
  /// values.
  SampleFrequency(String)
  /// The subject to deliver messages to. Note, setting this field implicitly
  /// decides whether the consumer is push or pull-based. With a deliver subject,
  /// the server will push messages to client subscribed to this subject.
  DeliverSubject(String)
  /// The queue group name which, if specified, is then used to distribute the
  /// messages between the subscribers to the consumer. This is analogous to a
  /// queue group in core NATS.
  DeliverGroup(String)
  /// Delivers only the headers of messages in the stream and not the bodies.
  /// Additionally adds Nats-Msg-Size header to indicate the size of the removed
  /// payload.
  HeadersOnly
}

/// The requirement of client acknowledgements.
///
pub type AckPolicy {
  /// The default policy. It means that each individual message must be
  /// acknowledged. It is recommended to use this mode, as it provides
  /// the most reliability and functionality.
  AckExplicit
  /// You do not have to ack any messages, the server will assume ack on
  /// delivery.
  AckNone
  /// If you receive a series of messages, you only have to ack the last
  /// one you received. All the previous messages received are automatically
  /// acknowledged at the same time.
  AckAll
}

/// Available delivier policies to select the point in the stream to
/// start consuming from.
///
pub type DeliverPolicy {
  /// The default policy. The consumer will start receiving from the earliest
  /// available message.
  DeliverAll
  /// When first consuming messages, the consumer will start receiving messages
  /// with the last message added to the stream, or the last message in the
  /// stream that matches the consumer's filter subject if defined.
  DeliverLast
  /// When first consuming messages, start with the latest one for each filtered
  /// subject currently in the stream.
  DeliverLastPerSubject
  /// When first consuming messages, the consumer will only start receiving
  /// messages that were created after the consumer was created.
  DeliverNew
  /// When first consuming messages, start at the first message having the
  /// sequence number or the next one available.
  DeliverByStartSequence(Int)
  /// When first consuming messages, start with messages on or after this time.
  DeliverByStartTime(String)
}

/// The policy to control how to replay messages from a stream.
///
pub type ReplayPolicy {
  /// The default policy. The messages will be pushed to the client as fast as
  /// possible while adhering to the Ack Policy, Max Ack Pending and the client's
  /// ability to consume those messages.
  ReplayInstant
  /// The messages in the stream will be pushed to the client at the same rate
  /// that they were originally received, simulating the original timing of messages.
  ReplayOriginal
}

pub type ConsumerInfo {
  ConsumerInfo(
    stream: String,
    name: String,
    created: String,
    config: ConsumerConfig,
    delivered: SequenceInfo,
    ack_floor: SequenceInfo,
    num_ack_pending: Int,
    num_redelivered: Int,
    num_waiting: Int,
    num_pending: Int,
  )
}

/// Avaialble config options for a single consumer.
///
pub type ConsumerConfig {
  ConsumerConfig(
    durable_name: Option(String),
    description: Option(String),
    filter_subject: Option(String),
    ack_policy: AckPolicy,
    ack_wait: Option(Int),
    deliver_policy: DeliverPolicy,
    inactive_threshold: Option(Int),
    max_ack_pending: Option(Int),
    max_deliver: Option(Int),
    replay_policy: ReplayPolicy,
    num_replicas: Option(Int),
    sample_freq: Option(String),
    deliver_subject: Option(String),
    deliver_group: Option(String),
    headers_only: Option(Bool),
  )
}

pub type SequenceInfo {
  SequenceInfo(consumer_seq: Int, stream_seq: Int)
}

//               //
// Consumer Info //
//               //

/// Get info about a consumer by stream and name.
///
pub fn info(
  conn: Connection,
  stream: String,
  name: String,
) -> Result(ConsumerInfo, JetstreamError) {
  let subject = consumer_prefix <> ".INFO." <> stream <> "." <> name

  case glats.request(conn, subject, "", 1000) {
    Ok(msg) -> decode_info(msg.body)
    // TODO: handle properly
    Error(_) -> Error(jetstream.ConsumerNotFound(""))
  }
}

external fn decode_consumer_info_data(
  data: Map(String, Dynamic),
) -> Result(ConsumerInfo, #(Int, String)) =
  "Elixir.Glats.Jetstream" "decode_consumer_info_data"

fn decode_info(body: String) {
  let decoder = dynamic.map(dynamic.string, dynamic.dynamic)

  json.decode(body, decoder)
  |> result.map(decode_consumer_info_data)
  |> result.map_error(fn(_) { #(-1, "decode error") })
  |> result.flatten
  |> result.map_error(js.map_code_to_error)
}

//                 //
// Create Consumer //
//                 //

/// Creates a new consumer
///
pub fn create(conn: Connection, stream: String, opts: List(ConsumerOption)) {
  let durable_name =
    list.find(
      opts,
      fn(o) {
        case o {
          DurableName(_) -> True
          _ -> False
        }
      },
    )

  // Check if opts include durable name.
  let subject = case durable_name {
    Ok(DurableName(name)) ->
      consumer_prefix <> ".CREATE." <> stream <> "." <> name
    Error(Nil) -> consumer_prefix <> ".CREATE." <> stream
  }

  let body = consumer_options_to_json(stream, opts)

  case glats.request(conn, subject, body, 1000) {
    Ok(msg) -> decode_info(msg.body)
    // TODO: handle properly
    Error(_) -> Error(jetstream.ConsumerNotFound(""))
  }
}

//                 //
// Delete Consumer //
//                 //

/// Deletes a consumer
///
pub fn delete(conn: Connection, stream: String, name: String) {
  let subject = consumer_prefix <> ".DELETE." <> stream <> "." <> name

  case glats.request(conn, subject, "", 1000) {
    Ok(msg) -> decode_delete(msg.body)
    // TODO: use actual descriptive error
    Error(_) -> Error(jetstream.StreamNotFound(""))
  }
}

external fn decode_delete_data(
  data: Map(String, Dynamic),
) -> Result(Nil, #(Int, String)) =
  "Elixir.Glats.Jetstream" "decode_delete_data"

fn decode_delete(body: String) -> Result(Nil, JetstreamError) {
  let decoder = dynamic.map(dynamic.string, dynamic.dynamic)

  json.decode(body, decoder)
  |> result.map(decode_delete_data)
  |> result.map_error(fn(_) { #(-1, "decode error") })
  |> result.flatten
  |> result.map_error(js.map_code_to_error)
}

//                //
// Consumer Names //
//                //

/// Get list of consumer names in a stream.
///
pub fn names(conn: Connection, stream: String) {
  let subject = consumer_prefix <> ".NAMES." <> stream

  case glats.request(conn, subject, "", 1000) {
    Ok(msg) -> decode_names(msg.body)
    // TODO: use actual descriptive error
    Error(_) -> Error(jetstream.StreamNotFound(""))
  }
}

external fn decode_consumer_names_data(
  data: Map(String, Dynamic),
) -> Result(Nil, #(Int, String)) =
  "Elixir.Glats.Jetstream" "decode_consumer_names_data"

fn decode_names(body: String) -> Result(Nil, JetstreamError) {
  let decoder = dynamic.map(dynamic.string, dynamic.dynamic)

  json.decode(body, decoder)
  |> result.map(decode_consumer_names_data)
  |> result.map_error(fn(_) { #(-1, "decode error") })
  |> result.flatten
  |> result.map_error(js.map_code_to_error)
}

//                      //
// Request Next Message //
//                      //

/// Options for `request_next_message`.
pub type RequestMessageOption {
  /// The number of messages to receive. Defaults to 1.
  Batch(Int)
  /// Get an empty message immediately if no new ones exist for
  /// the consumer.
  NoWait
  /// Expiry time for the request in nanoseconds.
  Expires(Int)
}

/// Request the next message for a pull subscription.
///
pub fn request_next_message(sub: Subscription, opts: List(RequestMessageOption)) {
  case sub {
    PullSubscription(conn, _, stream, consumer, inbox) ->
      do_req_next_msg(conn, stream, consumer, inbox, opts)
    _ ->
      Error(jetstream.PullConsumerRequired(
        "request_next_message only works on pull consumers",
      ))
  }
}

fn do_req_next_msg(
  conn: Connection,
  stream: String,
  consumer: String,
  inbox: String,
  opts: List(RequestMessageOption),
) {
  let subject = consumer_prefix <> ".MSG.NEXT." <> stream <> "." <> consumer

  // Create a further random subject for this single request
  let reply_to = inbox <> "." <> util.random_string(6)

  glats.publish_message(
    conn,
    Message(
      subject: subject,
      headers: map.new(),
      reply_to: Some(reply_to),
      body: make_req_body(opts),
    ),
  )
  |> result.map_error(fn(err) { jetstream.Unknown(-1, string.inspect(err)) })
}

fn make_req_body(opts: List(RequestMessageOption)) {
  [#("batch", json.int(1))]
  |> map.from_list
  |> list.fold(opts, _, apply_req_opt)
  |> map.to_list
  |> json.object
  |> json.to_string
}

fn apply_req_opt(prev: Map(String, Json), opt: RequestMessageOption) {
  case opt {
    Batch(size) -> map.insert(prev, "batch", json.int(size))
    Expires(time) -> map.insert(prev, "expires", json.int(time))
    NoWait -> map.insert(prev, "no_wait", json.bool(True))
  }
}

//           //
// Subscribe //
//           //

/// Subscribe to a subject in a stream.
///
/// - If no option is provided it will attempt to look up a stream
///   by the subject and create an ephemeral consumer for the
///   subscription.
/// - If `Bind("stream", "consumer")` is provided it will subsribe
///   to the stream and existing consumer, failing if either do not
///   exist.
/// - If `BindStream("stream")` is provided it will not attempt to
///   lookup the stream by subject but creates an ephemeral consumer
///   for the subscription.
///
/// In the cases where an ephemeral consumer will be created
/// `With(ConsumerOption)` can be provided to configure it.
///
pub fn subscribe(
  conn: Connection,
  subscriber: Subject(SubscriptionMessage),
  subject: String,
  opts: List(SubscriptionOption),
) {
  use stream <- result.then(find_stream(conn, subject, opts))
  use consumer <- result.then(find_consumer(conn, stream, subject, opts))

  case consumer.config.deliver_subject {
    None -> pull_subscribe(conn, subscriber, stream, consumer.name)
    Some(subj) ->
      push_subscribe(conn, subscriber, subj, consumer.config.deliver_group)
  }
}

fn pull_subscribe(
  conn: Connection,
  subscriber: Subject(SubscriptionMessage),
  stream: String,
  consumer: String,
) {
  // Create a random inbox for the pull subscription
  let inbox = util.random_inbox("")

  // Subscribe to the inbox subject
  glats.subscribe(conn, subscriber, inbox <> ".*")
  |> result.map(PullSubscription(conn, _, stream, consumer, inbox))
  |> result.map_error(fn(_) {
    jetstream.Unknown(-1, "unknown subscription error")
  })
}

fn push_subscribe(
  conn: Connection,
  subscriber: Subject(SubscriptionMessage),
  subject: String,
  group: Option(String),
) {
  // Subscribe to the deliver subject of the push consumer
  case group {
    // Choose a queue subscription or a regular subscription
    Some(group) -> glats.queue_subscribe(conn, subscriber, subject, group)
    None -> glats.subscribe(conn, subscriber, subject)
  }
  |> result.map(PushSubscription(conn, _))
  |> result.map_error(fn(_) {
    jetstream.Unknown(-1, "unknown subscription error")
  })
}

fn find_stream(
  conn: Connection,
  subject: String,
  opts: List(SubscriptionOption),
) {
  let stream =
    list.find_map(
      opts,
      fn(opt) {
        case opt {
          Bind(stream, _) -> Ok(stream)
          BindStream(stream) -> Ok(stream)
          _ -> Error(Nil)
        }
      },
    )

  case stream {
    Ok(stream) -> Ok(stream)
    Error(Nil) -> stream.find_stream_name_by_subject(conn, subject)
  }
}

fn find_consumer(
  conn: Connection,
  stream: String,
  subject: String,
  opts: List(SubscriptionOption),
) {
  let consumer =
    list.find_map(
      opts,
      fn(opt) {
        case opt {
          Bind(_, consumer) -> Ok(consumer)
          _ -> Error(Nil)
        }
      },
    )

  case consumer {
    Ok(consumer) -> info(conn, stream, consumer)
    Error(Nil) ->
      list.filter_map(
        opts,
        fn(opt) {
          case opt {
            With(o) -> Ok(o)
            _ -> Error(Nil)
          }
        },
      )
      |> ensure_consumer(conn, stream, subject, _)
  }
}

fn ensure_consumer(conn, stream, subject: String, opts: List(ConsumerOption)) {
  let opts = case
    list.find(
      opts,
      fn(opt) {
        case opt {
          FilterSubject(_) -> True
          _ -> False
        }
      },
    )
  {
    Ok(_) -> opts
    Error(Nil) ->
      opts
      |> list.prepend(FilterSubject(subject))
  }

  // Try to create a consumer
  create(conn, stream, opts)
}

//                                  //
// Consumer config building helpers //
//                                  //

fn consumer_options_to_json(stream: String, opts: List(ConsumerOption)) {
  [
    #("stream_name", json.string(stream)),
    #(
      "config",
      apply_consumer_options([], opts)
      |> json.object,
    ),
  ]
  |> json.object
  |> json.to_string
}

fn apply_consumer_options(
  prev: List(#(String, Json)),
  opts: List(ConsumerOption),
) {
  list.fold(opts, prev, apply_consumer_option)
}

fn apply_consumer_option(prev: List(#(String, Json)), opt: ConsumerOption) {
  // DeliverPolicy is kind of special as it will contain extra keys within
  // in certain cases (`DeliverByStartSequence(Int)` and `DeliverByStartTime(String)`)
  // so we need to handle that differently than the rest.
  // Maybe this can be done better?
  case opt {
    DeliverPolicy(pol) ->
      deliver_pol_to_list(pol)
      |> list.append(prev, _)
    AckPolicy(pol) ->
      #(
        "ack_policy",
        ack_pol_to_string(pol)
        |> json.string,
      )
      |> list.prepend(prev, _)
    ReplayPolicy(pol) ->
      #(
        "replay_policy",
        replay_pol_to_string(pol)
        |> json.string,
      )
      |> list.prepend(prev, _)
    DurableName(name) ->
      #("durable_name", json.string(name))
      |> list.prepend(prev, _)
    Description(desc) ->
      #("description", json.string(desc))
      |> list.prepend(prev, _)
    FilterSubject(subj) ->
      #("filter_subject", json.string(subj))
      |> list.prepend(prev, _)
    AckWait(num) ->
      #("ack_wait", json.int(num))
      |> list.prepend(prev, _)
    InactiveThreshold(num) ->
      #("inactive_threshold", json.int(num))
      |> list.prepend(prev, _)
    MaxAckPending(num) ->
      #("max_ack_pending", json.int(num))
      |> list.prepend(prev, _)
    MaxDeliver(num) ->
      #("max_deliver", json.int(num))
      |> list.prepend(prev, _)
    NumReplicas(num) ->
      #("num_replicas", json.int(num))
      |> list.prepend(prev, _)
    SampleFrequency(freq) ->
      #("sample_freq", json.string(freq))
      |> list.prepend(prev, _)
    DeliverSubject(subj) ->
      #("deliver_subject", json.string(subj))
      |> list.prepend(prev, _)
    DeliverGroup(group) ->
      #("deliver_group", json.string(group))
      |> list.prepend(prev, _)
    HeadersOnly ->
      #("headers_only", json.bool(True))
      |> list.prepend(prev, _)
  }
}

fn ack_pol_to_string(pol: AckPolicy) {
  case pol {
    AckExplicit -> "explicit"
    AckNone -> "none"
    AckAll -> "all"
  }
}

fn deliver_pol_to_list(pol: DeliverPolicy) {
  case pol {
    DeliverAll -> [#("deliver_policy", json.string("all"))]
    DeliverLast -> [#("deliver_policy", json.string("last"))]
    DeliverLastPerSubject -> [
      #("deliver_policy", json.string("last_per_subject")),
    ]
    DeliverNew -> [#("deliver_policy", json.string("new"))]
    DeliverByStartSequence(seq) -> [
      #("deliver_policy", json.string("by_start_sequence")),
      #("opt_start_seq", json.int(seq)),
    ]
    DeliverByStartTime(time) -> [
      #("deliver_policy", json.string("by_start_time")),
      #("opt_start_time", json.string(time)),
    ]
  }
}

fn replay_pol_to_string(pol: ReplayPolicy) {
  case pol {
    ReplayInstant -> "instant"
    ReplayOriginal -> "original"
  }
}
