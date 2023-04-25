defmodule Glats.Jetstream do
  ############
  ## Stream ##
  ############
  # Catches stream info when there's an error.
  def decode_stream_info_data(%{"error" => %{"err_code" => code, "description" => description}}) do
    {:error, {code, description}}
  end
  # Decodes stream info.
  def decode_stream_info_data(%{"created" => created, "config" => config, "state" => state}) do
    {:ok,
      {:stream_info,
        created,
        decode_stream_config(config),
        decode_stream_state(state),
      }
    }
  end
  # Decodes stream config.
  def decode_stream_config(config) do
    {:stream_config,
      Map.get(config, "name"),
      Map.get(config, "subjects"),
      optional(Map.get(config, "retention")),
      optional(Map.get(config, "max_consumers")),
      optional(Map.get(config, "max_msgs")),
      optional(Map.get(config, "max_bytes")),
      optional(Map.get(config, "max_age")),
      optional(Map.get(config, "max_msgs_per_subject")),
      optional(Map.get(config, "max_msg_size")),
      optional(Map.get(config, "discard")),
      optional(Map.get(config, "storage")),
      optional(Map.get(config, "num_replicas")),
      optional(Map.get(config, "duplicate_window")),
      optional(Map.get(config, "allow_direct")),
      optional(Map.get(config, "mirror_direct")),
      optional(Map.get(config, "sealed")),
      optional(Map.get(config, "deny_delete")),
      optional(Map.get(config, "deny_purge")),
      optional(Map.get(config, "allow_rollup_hdrs")),
    }
  end
  # Decodes stream state.
  def decode_stream_state(state) do
    {:stream_state,
      Map.get(state, "messages"),
      Map.get(state, "bytes"),
      Map.get(state, "first_seq"),
      Map.get(state, "first_ts"),
      Map.get(state, "last_seq"),
      Map.get(state, "last_ts"),
      Map.get(state, "consumer_count"),
    }
  end

  # Catches stream deletion when there's an error.
  def decode_stream_delete_data(%{"error" => %{"err_code" => code, "description" => description}}) do
    {:error, {code, description}}
  end
  # Decodes a stream deletion response.
  def decode_stream_delete_data(%{"success" => true}) do
    {:ok, nil}
  end

  # Catches stream purge when there's an error.
  def decode_stream_purge_data(%{"error" => %{"err_code" => code, "description" => description}}) do
    {:error, {code, description}}
  end
  # Decodes a stream purge response.
  def decode_stream_purge_data(%{"success" => true, "purged" => count}) do
    {:ok, count}
  end

  # Decode a raw message from stream when error.
  def decode_raw_stream_message_data(%{"error" => %{"err_code" => code, "description" => description}}) do
    {:error, {code, description}}
  end
  # Decode a raw message from stream.
  def decode_raw_stream_message_data(%{"message" => message}) do
    {:ok,
      {:raw_stream_message,
        Map.get(message, "subject"),
        Map.get(message, "seq"),
        optional(Map.get(message, "hdrs")),
        Map.get(message, "data"),
        Map.get(message, "time"),
      }
    }
  end

  # Catches error in response for names
  def decode_stream_names_data(%{"error" => %{"err_code" => code, "description" => description}}) do
    {:error, {code, description}}
  end
  # Decodes names response
  def decode_stream_names_data(%{"streams" => streams}) do
    case streams do
      :null -> {:error, {10059, "no stream with that subject found"}}
      _ -> {:ok, streams || []}
    end
  end

  ##############
  ## Consumer ##
  ##############
  # Catches consumer info when there's an error.
  def decode_consumer_info_data(%{"error" => %{"err_code" => code, "description" => description}}) do
    {:error, {code, description}}
  end
  # Decodes consumer info.
  def decode_consumer_info_data(%{
    "stream_name" => stream,
    "name" => name,
    "created" => created,
    "config" => config,
    "delivered" => delivered,
    "ack_floor" => ack_floor,
    "num_ack_pending" => num_ack_pending,
    "num_redelivered" => num_redelivered,
    "num_waiting" => num_waiting,
    "num_pending" => num_pending,
  }) do
    {:ok,
      {:consumer_info,
        stream,
        name,
        created,
        decode_consumer_config(config),
        decode_sequence_info(delivered),
        decode_sequence_info(ack_floor),
        num_ack_pending,
        num_redelivered,
        num_waiting,
        num_pending,
      }
    }
  end
  # Decodes stream config.
  def decode_consumer_config(config) do
    {:consumer_config,
      optional(Map.get(config, "durable_name")),
      optional(Map.get(config, "description")),
      optional(Map.get(config, "filter_subject")),
      decode_ack_policy(Map.get(config, "ack_policy")),
      optional(Map.get(config, "ack_wait")),
      decode_deliver_policy(config),
      optional(Map.get(config, "inactive_threshold")),
      optional(Map.get(config, "max_ack_pending")),
      optional(Map.get(config, "max_pending")),
      decode_replay_policy(Map.get(config, "replay_policy")),
      optional(Map.get(config, "num_replicas")),
      optional(Map.get(config, "sample_freq")),
    }
  end
  # Decodes ack policy
  def decode_ack_policy("all") do :ack_all end
  def decode_ack_policy("none") do :ack_none end
  def decode_ack_policy("explicit") do :ack_explicit end
  # Decodes deliver policy
  def decode_deliver_policy(%{"deliver_policy" => "all"}) do :deliver_all end
  def decode_deliver_policy(%{"deliver_policy" => "last"}) do :deliver_last end
  def decode_deliver_policy(%{"deliver_policy" => "last_per_subject"}) do
    :deliver_last_per_subject
  end
  def decode_deliver_policy(%{"deliver_policy" => "new"}) do :deliver_new end
  def decode_deliver_policy(%{
    "deliver_policy" => "by_start_sequence",
    "opt_start_seq" => seq,
  }) do
    {:deliver_by_start_sequence, seq}
  end
  def decode_deliver_policy(%{
    "deliver_policy" => "by_start_time",
    "opt_start_time" => time,
  }) do
    {:deliver_by_start_time, time}
  end
  # Decodes replay policy
  def decode_replay_policy("instant") do :replay_instant end
  def decode_replay_policy("original") do :replay_original end
  # Decodes sequence info
  def decode_sequence_info(%{"consumer_seq" => consumer_seq, "stream_seq" => stream_seq}) do
    {:sequence_info,
      consumer_seq,
      stream_seq,
    }
  end

  # Catches error in response for names
  def decode_consumer_names_data(%{"error" => %{"err_code" => code, "description" => description}}) do
    {:error, {code, description}}
  end
  # Decodes names response
  def decode_consumer_names_data(%{"consumers" => consumers}) do
    {:ok, consumers}
  end

  # Returns Some(val) or None depending on nil or not.
  defp optional(nil) do :none end
  defp optional(val) do {:some, val} end
end