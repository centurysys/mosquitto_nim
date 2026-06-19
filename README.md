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
- minimal nmqtt-compatible facade
- username/password authentication plumbing
- Will message plumbing
- TLS certificate configuration plumbing
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
Some cloud brokers require client certificate authentication; use `mqttTls()` or
the nmqtt-compatible `set_ssl_certificates()` path for client cert/key plumbing.

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

The compatibility facade intentionally preserves nmqtt-style queue-oriented
publish semantics:

- `publish()` returns after the publish command is accepted into the worker
  command queue.
- It does not wait for PUBACK or `PublishCompleted`.
- Publish completion is tracked separately through worker/highlevel events.

Currently supported in the compatibility facade:

- `newMqttCtx(clientId)`
- `set_host(host, port = 1883, sslOn = false)`
- `set_ping_interval(seconds)`
- `set_auth(username, password)`
- `set_ssl_certificates(certfile, keyfile)`
- `set_will(topic, message, qos = 0, retain = false)`
- `setProtocolVersion(mpv311 | mpv5)`
- `start()`
- `connect()`
- `disconnect()`
- `publish(topic, message, qos = 0, retain = false)`
- `publishV5(...)` as an extension API
- `subscribe(topic, qos, callback)`
- `subscribeV5(...)` as an extension API
- `unsubscribe(topic)`
- `isConnected()`
- `msgQueue()`

Not yet complete:

- Full nmqtt API parity
- Full MQTT v5 property coverage
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
