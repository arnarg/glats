defmodule Glats do
  # Decode server info response
  def decode_server_info(info) when is_map(info) do
    {:ok,
      {:server_info,
        Map.get(info, :server_id),
        Map.get(info, :server_name),
        Map.get(info, :version),
        Map.get(info, :go),
        Map.get(info, :host),
        Map.get(info, :port),
        Map.get(info, :headers),
        Map.get(info, :max_payload),
        Map.get(info, :proto),
        Map.get(info, :jetstream, false),
        optional(Map.get(info, :auth_required)),
      }
    }
  end
  def decode_server_info(_info) do
    {:error, :unknown}
  end

  # Returns Some(val) or None depending on nil or not.
  def optional(nil) do :none end
  def optional(val) do {:some, val} end
end
