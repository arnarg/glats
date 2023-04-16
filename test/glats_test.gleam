import gleam/option.{Some}
import gleam/map
import gleeunit
import gleeunit/should
import glats.{default_settings}
import glats/connection.{Settings}

pub fn main() {
  gleeunit.main()
}

pub fn with_host_test() {
  default_settings()
  |> glats.with_host("1.1.1.1")
  |> should.equal(Settings(..default_settings(), host: Some("1.1.1.1")))
}

pub fn with_port_test() {
  default_settings()
  |> glats.with_port(8080)
  |> should.equal(Settings(..default_settings(), port: Some(8080)))
}

pub fn with_ca_test() {
  default_settings()
  |> glats.with_ca("/tmp/ca.crt")
  |> should.equal(
    Settings(
      ..default_settings(),
      tls: Some(True),
      ssl_opts: Some(
        [#("cacertfile", "/tmp/ca.crt")]
        |> map.from_list(),
      ),
    ),
  )
}

pub fn with_client_cert_test() {
  default_settings()
  |> glats.with_client_cert("/tmp/client.crt", "/tmp/client.key")
  |> should.equal(
    Settings(
      ..default_settings(),
      tls: Some(True),
      ssl_opts: Some(
        [#("certfile", "/tmp/client.crt"), #("keyfile", "/tmp/client.key")]
        |> map.from_list(),
      ),
    ),
  )
}

pub fn with_both_cert_opts_test() {
  default_settings()
  |> glats.with_ca("/tmp/ca.crt")
  |> glats.with_client_cert("/tmp/client.crt", "/tmp/client.key")
  |> should.equal(
    Settings(
      ..default_settings(),
      tls: Some(True),
      ssl_opts: Some(
        [
          #("cacertfile", "/tmp/ca.crt"),
          #("certfile", "/tmp/client.crt"),
          #("keyfile", "/tmp/client.key"),
        ]
        |> map.from_list(),
      ),
    ),
  )
}

pub fn with_no_tls_test() {
  default_settings()
  |> glats.with_no_tls
  |> should.equal(Settings(..default_settings(), tls: Some(False)))
}
