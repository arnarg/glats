import gleam/map
import glats/jetstream.{JetstreamError}

// This maps an error code returned from Jetstream
// to an actual typed error in Gleam.
const code_mapping = [
  #(10_039, jetstream.JetstreamNotEnabledForAccount),
  #(10_076, jetstream.JetstreamNotEnabled),
  #(10_023, jetstream.InsufficientResources),
  #(10_058, jetstream.StreamNameInUse),
  #(10_059, jetstream.StreamNotFound),
  #(10_037, jetstream.NoMessageFound),
  #(10_014, jetstream.ConsumerNotFound),
  #(10_013, jetstream.ConsumerNameExists),
  #(10_105, jetstream.ConsumerAlreadyExists),
  #(10_071, jetstream.WrongLastSequence),
  #(10_003, jetstream.BadRequest),
]

pub fn map_code_to_error(data: #(Int, String)) -> JetstreamError {
  let err_map = map.from_list(code_mapping)

  case
    err_map
    |> map.get(data.0)
  {
    Ok(err) -> err(data.1)
    Error(Nil) -> jetstream.Unknown(data.0, data.1)
  }
}
