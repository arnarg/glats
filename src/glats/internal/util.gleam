import gleam/base

external fn strong_rand_bytes(Int) -> BitString =
  "crypto" "strong_rand_bytes"

pub fn random_inbox(prefix: String) {
  let prefix = case prefix {
    "" -> "_INBOX."
    p -> p
  }

  prefix <> random_string(16)
}

pub fn random_string(len: Int) {
  strong_rand_bytes(len)
  |> base.encode64(False)
}
