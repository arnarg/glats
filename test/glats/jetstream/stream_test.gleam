import gleeunit/should
import gleam/option.{None, Some}
import glats/jetstream
import glats/jetstream/stream.{StreamConfig}

pub fn new_config_test() {
  stream.new_config("test", ["test.>"])
  |> should.equal(StreamConfig(
    name: "test",
    subjects: ["test.>"],
    retention: None,
    max_consumers: None,
    max_msgs: None,
    max_bytes: None,
    max_age: None,
    max_msgs_per_subject: None,
    max_msg_size: None,
    discard: None,
    storage: None,
    num_replicas: None,
    duplicate_window: None,
    allow_direct: None,
    mirror_direct: None,
    sealed: None,
    deny_delete: None,
    deny_purge: None,
    allow_rollup_hdrs: None,
  ))
}

pub fn with_retention_test() {
  stream.new_config("test", ["test.>"])
  |> stream.with_retention(jetstream.LimitsPolicy)
  |> should.equal(
    StreamConfig(
      ..stream.new_config("test", ["test.>"]),
      retention: Some("limits"),
    ),
  )
}

pub fn with_storage_test() {
  stream.new_config("test", ["test.>"])
  |> stream.with_storage(jetstream.FileStorage)
  |> should.equal(
    StreamConfig(..stream.new_config("test", ["test.>"]), storage: Some("file")),
  )
}
