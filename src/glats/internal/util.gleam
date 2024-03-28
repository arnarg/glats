import gleam/bit_array.{base64_encode}
import gleam/crypto.{strong_random_bytes}

pub fn random_inbox(prefix: String) {
  let prefix = case prefix {
    "" -> "_INBOX."
    p -> p
  }

  prefix <> random_string(16)
}

pub fn random_string(len: Int) {
  strong_random_bytes(len)
  |> base64_encode(False)
}
