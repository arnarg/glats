defmodule Glats.Jetstream do
  # Catches stream info when there's an error.
  def decode_info_data(%{"error" => %{"err_code" => 10059}}) do
    {:error, :stream_not_found}
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
      Map.get(config, "retention"),
      Map.get(config, "max_consumers"),
      Map.get(config, "max_msgs"),
      Map.get(config, "max_bytes"),
      Map.get(config, "max_age"),
      Map.get(config, "max_msgs_per_subject"),
      Map.get(config, "max_msg_size"),
      Map.get(config, "discard"),
      Map.get(config, "storage"),
      Map.get(config, "num_replicas"),
      Map.get(config, "duplicate_window"),
      Map.get(config, "allow_direct"),
      Map.get(config, "mirror_direct"),
      Map.get(config, "sealed"),
      Map.get(config, "deny_delete"),
      Map.get(config, "deny_purge"),
      Map.get(config, "allow_rollup_hdrs"),
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
end
