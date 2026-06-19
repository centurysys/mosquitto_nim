# mosquitto_nim

`mosquitto_nim` is a Nim wrapper around the Mosquitto MQTT C client library.

The current implementation is built in layers:

- `lowlevel/`: thin, safe wrappers around `libmosquitto`
- `worker/`: a threadtools-based MQTT worker that owns the `libmosquitto` handle
- `highlevel/`: asyncdispatch-facing client, dispatcher, and nmqtt-compatible facade
- `lowlevel/bindings/generated/`: Futhark-generated raw C bindings

The main design goal is to keep raw C pointers, callback trampolines, and
`libmosquitto` ownership out of application code while still exposing enough of
Mosquitto to support MQTT v3.1.1, MQTT v5, TLS, authentication, Will messages,
and nmqtt-style asynchronous usage.

## Status

This package is under active development.

Currently implemented:

- Dynamic binding to `libmosquitto.so.1`
- Basic lowlevel client creation/destruction
- Manual-loop lowlevel connect / disconnect / publish / subscribe / unsubscribe
- Nim-owned copy of incoming MQTT messages
- libmosquitto callback trampolines
- threadtools-based MQTT worker
- asyncdispatch bridge for worker events
- highlevel client facade
- highlevel message dispatcher
- nmqtt-style facade with mosquitto_nim extensions
- username/password authentication plumbing
- Will message plumbing
- TLS configuration plumbing with OS trust store, explicit CA, mTLS client cert/key, and explicit insecure mode
- MQTT protocol version selection
- MQTT v5 CONNECT properties:
  - Session Expiry Interval
  - Receive Maximum
  - Maximum Packet Size
  - Request Problem Information
  - User Property
- MQTT v5 PUBLISH properties:
  - User Property
  - Response Topic
  - Correlation Data
  - Message Expiry Interval
  - Content Type
  - Payload Format Indicator
- MQTT v5 CONNACK/control property copy:
  - Assigned Client Identifier
  - Server Keep Alive
  - Receive Maximum
  - Maximum Packet Size
  - Reason String
  - Response Information
  - Server Reference
  - User Property

## Requirements

Nim dependencies:

```nim
requires "nim >= 2.2.10"
requires "results >= 0.5.1"
requires "threading >= 0.2.1"
requires "threadtools >= 0.1.0"
```

System dependency:

- Debian/Ubuntu: `libmosquitto-dev`
- Alpine: `mosquitto-dev`

At runtime, `libmosquitto.so.1` must be available.

## TLS note

MQTT over TLS is handled by `libmosquitto`, not by Nim's standard SSL modules.
Therefore `mosquitto_nim` itself does not require compiling Nim with `-d:ssl`.

If the application separately uses Nim's `std/httpclient`, `std/net`, or
`std/asyncnet` for HTTPS/TLS communication, that application code may still need
`-d:ssl`.

With the current dynlib approach, `libssl` / `libcrypto` are resolved through
`libmosquitto.so.1` and its dynamic dependencies.

For public cloud brokers, the server certificate is normally signed by a public
CA. Self-signed certificates are mainly needed for local/private broker tests.
Use `mqttTlsWithOsTrustStore()` / `set_tls_os_certs()` for public CA based
brokers, `mqttTlsWithCa()` / `set_tls_ca()` for private CA roots, and
`mqttTlsClientCertificate()` / `set_ssl_certificates()` for mTLS client
certificate authentication. `insecure` / `set_tls_insecure()` is available only
for explicit development or local broker testing.

## Quick example: nmqtt-compatible facade

```nim
import std/asyncdispatch
import mosquitto_nim/highlevel/nmqtt_compat

proc main() {.async.} =
  let ctx = newMqttCtx("mosquitto-nim-example")

  ctx.set_host("127.0.0.1", 1883)
  ctx.set_ping_interval(30)

  await ctx.start()

  await ctx.subscribe("demo/+", 0) do (topic, message: string):
    echo topic, ": ", message

  await ctx.publish("demo/hello", "hello from mosquitto_nim", qos = 0)

  await sleepAsync(500)
  await ctx.disconnect()

waitFor main()
```

## MQTT v5 example

```nim
import std/asyncdispatch
import mosquitto_nim
import mosquitto_nim/highlevel/nmqtt_compat

proc main() {.async.} =
  let ctx = newMqttCtx("mosquitto-nim-v5-example")

  ctx.set_host("127.0.0.1", 1883)
  ctx.setProtocolVersion(mpv5)

  var connectProps = noConnectProperties()
  connectProps.setSessionExpiryInterval(3600'u32)
  connectProps.setReceiveMaximum(16'u16)
  connectProps.setRequestProblemInformation(true)
  discard ctx.setConnectProperties(connectProps)

  await ctx.start()

  var subProps = noSubscribeProperties()
  subProps.setSubscriptionIdentifier(1)
  subProps.addUserProperty("route", "demo")

  await ctx.subscribeV5("demo/v5", subProps, 0) do (topic, message: string):
    echo topic, ": ", message

  await ctx.publishV5(
    topic = "demo/v5",
    message = "hello with properties",
    qos = 0,
    properties = @[
      userProperty("trace-id", "abc123"),
      responseTopic("demo/response"),
      correlationData(@[1'u8, 2, 3, 4]),
    ],
  )

  await sleepAsync(500)
  await ctx.disconnect()

waitFor main()
```

## nmqtt compatibility scope

`highlevel/nmqtt_compat` is not intended to be a byte-for-byte reimplementation
of every nmqtt API. It is a **nmqtt-style facade that preserves the basic API
shape and default behavior while adding mosquitto_nim-specific practical
extensions**.

The compatibility facade intentionally preserves nmqtt-style queue-oriented
publish semantics:

- `publish()` returns after the publish command is accepted into the worker
  command queue.
- It does not wait for PUBACK or `PublishCompleted`.
- Publish completion is tracked separately through worker/highlevel events.
- After `start()`, reconnect and offline publish queueing are enabled by
  default in the compatibility facade to match nmqtt-style usage.

### nmqtt-compatible API

These APIs are intended to match the original nmqtt API shape or behavior.

- `newMqttCtx(clientId)`
- `set_host(host, port = 1883, sslOn = false)`
- `set_ping_interval(seconds)`
- `set_auth(username, password)`
- `set_ssl_certificates(certfile, keyfile)`
- `set_will(topic, message, qos = 0, retain = false)`
- `start()`
- `connect()`
- `disconnect()`
- `publish(topic, message, qos = 0, retain = false)`
- `subscribe(topic, qos, callback)`
- `unsubscribe(topic)`
- `isConnected()`
- `msgQueue()`

### mosquitto_nim extension API

The following APIs are mosquitto_nim extensions for `libmosquitto`, MQTT v5,
reconnect, offline queueing, TLS, and diagnostics.

- MQTT protocol version:
  - `setProtocolVersion(mpv311 | mpv5)`
- MQTT v5 CONNECT metadata:
  - `setConnectProperties(...)`
  - `connectProperties()`
  - `clearConnectProperties()`
- MQTT v5 PUBLISH metadata:
  - `publishV5(...)`
  - `MqttPublishProperties`
- MQTT v5 SUBSCRIBE metadata:
  - `subscribeV5(...)`
  - `MqttSubscribeProperties`
- TLS trust source / test configuration:
  - `set_tls_os_certs()`
  - `set_tls_ca(cafile)`
  - `set_tls_capath(capath)`
  - `set_tls_insecure(insecure = true)`
  - `tlsConfig()`
  - `setTls(...)`
  - `clearTls()`
- Reconnect / offline queue policy:
  - `setReconnectPolicy(...)`
  - `enableReconnect(...)`
  - `disableReconnect()`
  - `setOfflineQueuePolicy(...)`
  - `enableOfflineQueue(...)`
  - `disableOfflineQueue()`
- State and diagnostics:
  - `currentState()`
  - `pendingOperations()`
  - `queueSnapshot()`
  - `lastConnectReasonCode()`
  - `lastConnectProperties()`

Not yet complete:

- nmqtt compatibility matrix
- Maintaining the split between original nmqtt-compatible APIs and
  mosquitto_nim extension APIs
- Additional MQTT v5 property coverage
- Advanced reconnect/offline-queue edge-case coverage
- TLS integration tests against an actual TLS broker
- WebSocket / WSS support

## Design summary

`libmosquitto` callbacks are not used to call application callbacks directly.

Instead:

```text
libmosquitto callback
  -> copy C-owned data into Nim-owned values
  -> worker event queue
  -> asyncdispatch bridge
  -> highlevel dispatcher
  -> application callback
```

This keeps application callbacks on the asyncdispatch side and keeps the
`libmosquitto` handle owned by the MQTT worker thread.

## License

MIT. See `LICENSE`.
