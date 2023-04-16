defmodule Glats do
  # Converts expected map from Gnat to a message record defined in gleam.
  def convert_msg({:msg, map}) do
    convert_bare_msg(map)
  end
  # Matches when headers are present.
  def convert_bare_msg(%{topic: topic, body: body, reply_to: reply_to, headers: headers}) do
    {:ok, {:message, topic, headers, optional(reply_to), body}}
  end
  # Matches when headers are not present.
  def convert_bare_msg(%{topic: topic, body: body, reply_to: reply_to}) do
    {:ok, {:message, topic, Map.new(), optional(reply_to), body}}
  end

  def optional(nil) do :none end
  def optional(val) do {:some, val} end
end
