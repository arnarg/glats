import gleam/option.{Some}
import gleam/map
import gleeunit/should
import glats/settings.{Settings, defaults}

pub fn set_host_test() {
  defaults()
  |> settings.set_host("1.1.1.1")
  |> should.equal(Settings(..defaults(), host: Some("1.1.1.1")))
}

pub fn set_port_test() {
  defaults()
  |> settings.set_port(8080)
  |> should.equal(Settings(..defaults(), port: Some(8080)))
}

pub fn set_ca_test() {
  defaults()
  |> settings.set_ca("/tmp/ca.crt")
  |> should.equal(
    Settings(
      ..defaults(),
      tls: Some(True),
      ssl_opts: Some(
        [#("cacertfile", "/tmp/ca.crt")]
        |> map.from_list(),
      ),
    ),
  )
}

pub fn set_client_cert_test() {
  defaults()
  |> settings.set_client_cert("/tmp/client.crt", "/tmp/client.key")
  |> should.equal(
    Settings(
      ..defaults(),
      tls: Some(True),
      ssl_opts: Some(
        [#("certfile", "/tmp/client.crt"), #("keyfile", "/tmp/client.key")]
        |> map.from_list(),
      ),
    ),
  )
}

pub fn set_both_cert_opts_test() {
  defaults()
  |> settings.set_ca("/tmp/ca.crt")
  |> settings.set_client_cert("/tmp/client.crt", "/tmp/client.key")
  |> should.equal(
    Settings(
      ..defaults(),
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

pub fn set_no_tls_test() {
  defaults()
  |> settings.set_no_tls
  |> should.equal(Settings(..defaults(), tls: Some(False)))
}
