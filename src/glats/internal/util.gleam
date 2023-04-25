import gleam/base

external fn strong_rand_bytes(Int) -> BitString =
  "crypto" "strong_rand_bytes"

pub fn random_inbox(prefix: String) {
  let prefix = case prefix {
    "" -> "_INBOX."
    p -> p
  }

  let rand =
    strong_rand_bytes(16)
    |> base.encode64(False)

  prefix <> rand
}
