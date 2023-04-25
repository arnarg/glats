import gleam/base
import gleam/result
import gleam/string
import gleam/list
import gleam/map
import gleam/bit_string
import glats.{ConnectionError}
import glats/jetstream.{JetstreamError}

pub fn map_code_to_error(data: #(Int, String)) -> JetstreamError {
  case data.0 {
    10_039 -> jetstream.JetstreamNotEnabledForAccount(data.1)
    10_076 -> jetstream.JetstreamNotEnabled(data.1)
    10_023 -> jetstream.InsufficientResources(data.1)
    10_052 -> jetstream.InvalidStreamConfig(data.1)
    10_056 -> jetstream.StreamNameInSubjectDoesNotMatch(data.1)
    10_058 -> jetstream.StreamNameInUse(data.1)
    10_059 -> jetstream.StreamNotFound(data.1)
    10_110 -> jetstream.StreamPurgeNotAllowed(data.1)
    10_037 -> jetstream.NoMessageFound(data.1)
    10_014 -> jetstream.ConsumerNotFound(data.1)
    10_013 -> jetstream.ConsumerNameExists(data.1)
    10_105 -> jetstream.ConsumerAlreadyExists(data.1)
    10_071 -> jetstream.WrongLastSequence(data.1)
    10_003 -> jetstream.BadRequest(data.1)
    _ -> jetstream.Unknown(data.0, data.1)
  }
}

pub fn decode_b64_headers(hdrs: String) {
  use data <- result.then(
    base.decode64(hdrs)
    |> result.map(bit_string.to_string)
    |> result.flatten,
  )

  decode_headers(data)
}

pub fn decode_headers(hdrs: String) {
  hdrs
  |> string.split("\n")
  |> list.filter_map(decode_header)
  |> map.from_list
  |> Ok
}

fn decode_header(line: String) {
  case
    line
    |> string.split(": ")
  {
    [key, val] -> Ok(#(key, val))
    _ -> Error(Nil)
  }
}

pub fn map_glats_error_to_jetstream(err: ConnectionError) -> JetstreamError {
  case err {
    glats.Timeout -> jetstream.Timeout
    glats.NoResponders -> jetstream.NoResponders
    _ -> jetstream.Unknown(-1, "unknown error")
  }
}
