import gleam/map.{Map}
import gleam/dynamic.{Dynamic}
import gleam/option.{Option}
import gleam/list
import gleam/result
import gleam/json.{Json}
import glats.{Connection}
import glats/jetstream.{JetstreamError}
import glats/internal/js

const consumer_prefix = "$JS.API.CONSUMER"

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
