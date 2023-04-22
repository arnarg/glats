defmodule Glats.Jetstream do
  # Catches stream info when there's an error.
  def decode_info_data(%{"error" => %{"err_code" => code, "description" => description}}) do
    {:error, {code, description}}
  end
  # Decodes stream info.
  def decode_info_data(%{"created" => created, "config" => config, "state" => state}) do
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
  def decode_delete_data(%{"error" => %{"err_code" => code, "description" => description}}) do
    {:error, {code, description}}
  end
  # Decodes a stream deletion response.
  def decode_delete_data(%{"success" => true}) do
    {:ok, nil}
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

  # Returns Some(val) or None depending on nil or not.
  def optional(nil) do :none end
  def optional(val) do {:some, val} end
end
