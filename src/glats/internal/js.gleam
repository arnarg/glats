import glats/jetstream.{JetstreamError}

pub fn map_code_to_error(data: #(Int, String)) -> JetstreamError {
  case data.0 {
    10_039 -> jetstream.JetstreamNotEnabledForAccount(data.1)
    10_076 -> jetstream.JetstreamNotEnabled(data.1)
    10_023 -> jetstream.InsufficientResources(data.1)
    10_052 -> jetstream.InvalidStreamConfig(data.1)
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
