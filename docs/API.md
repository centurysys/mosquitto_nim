# mosquitto_nim API Guide

This guide describes the current public API shape of `mosquitto_nim`.

The library is intentionally layered:

```text
lowlevel/
  raw libmosquitto wrapper and safe C/Nim conversion

worker/
  threadtools-based worker that owns LowLevelClient

highlevel/
  asyncdispatch bridge, client facade, dispatcher, nmqtt compatibility
```

## 1. Lowlevel API

The lowlevel API wraps `libmosquitto` while hiding raw C pointers from callers.

Typical modules:

- `mosquitto_nim/lowlevel/types`
- `mosquitto_nim/lowlevel/errors`
- `mosquitto_nim/lowlevel/client`
- `mosquitto_nim/lowlevel/bridge`

### Result type

Lowlevel operations return `MqttResult[T]`.

Conceptually:

```nim
type
  MqttResult[T] = Result[T, MqttError]
  MqttOk = object
```

Errors are represented as Nim values instead of exceptions across thread and
callback boundaries.

### Core value types

Important public lowlevel values include:

- `MqttMessage`
- `MqttQos`
- `MqttProtocolVersion`
- `MqttWill`
- `MqttTlsConfig`
- `MqttProperty`
- `MqttProperties`

`MqttMessage` owns its topic, payload, and properties. It does not expose
pointers into `libmosquitto`.

### Protocol version

Supported protocol versions:

- `mpv311`
- `mpv5`

The default is `mpv311`.

### MQTT v5 properties

Currently supported:

- `userProperty(name, value)`
- `responseTopic(topic)`
- `correlationData(data)`
- `messageExpiryInterval(seconds)`
- `contentType(value)`
- `payloadFormatIndicatorUtf8()`
- `payloadFormatIndicatorUnspecified()`

These can be passed to `publishV5()` through highlevel or compatibility APIs.

## 2. Worker API

The worker layer owns the lowlevel client and runs `mosquitto_loop()` in the
worker thread.

The application side sends `MqttCommand` values and receives `MqttEvent`
values.

### Commands

Current command kinds include:

- Connect
- Disconnect
- Publish
- Subscribe
- Unsubscribe
- Stop

### Events

Current event kinds include:

- Connected
- Disconnected
- PublishAccepted
- PublishCompleted
- Subscribed
- Unsubscribed
- MessageReceived
- Error
- Stopped

### Publish event semantics

Publish events are intentionally split:

```text
PublishAccepted
  mosquitto_publish accepted the message and returned a mid

PublishCompleted
  libmosquitto on_publish callback fired
  for QoS 1 this corresponds to PUBACK
  for QoS 2 this corresponds to completion of the QoS 2 flow
```

This split is important for nmqtt compatibility. The nmqtt-style `publish()`
should not wait for PUBACK.

## 3. Highlevel client API

The highlevel client owns:

- `MqttWorker`
- `MqttAsyncBridge`
- `MqttDispatcher`

It lets applications send commands and await events without touching
threadtools queues directly.

Typical module:

```nim
import mosquitto_nim/highlevel/client
```

Main operations:

- `newMqttClient(...)`
- `connect(...)`
- `disconnect(...)`
- `publish(...)`
- `publishV5(...)`
- `subscribe(...)`
- `unsubscribe(...)`
- `nextEvent(...)`
- `drainEvents(...)`
- `dispatchNextEvent(...)`
- `dispatchDrainedEvents(...)`
- `requestStop(...)`
- `joinMqttClient(...)`

### Queue-oriented publish

`publish()` returns after the command is accepted into the worker command
queue. It does not wait for `PublishCompleted`.

Use events if completion tracking is needed.

## 4. Dispatcher API

The dispatcher maps MQTT topic filters to message handlers.

Typical module:

```nim
import mosquitto_nim/highlevel/dispatcher
```

Supported handler styles:

```nim
proc(msg: MqttMessage)
proc(msg: MqttMessage): Future[void]
```

Async handlers are awaited serially by default. This avoids running application
callbacks concurrently unless a future API explicitly opts into that behavior.

Topic filter matching supports normal MQTT subscription filters such as:

- `sensor/+/value`
- `device/#`

## 5. nmqtt compatibility facade

Typical module:

```nim
import mosquitto_nim/highlevel/nmqtt_compat
```

The goal is source-level familiarity with nmqtt while keeping the new
libmosquitto/threadtools implementation underneath.

### Supported API

Currently supported:

```nim
let ctx = newMqttCtx("client-id")

ctx.set_host("127.0.0.1", 1883)
ctx.set_ping_interval(30)
ctx.set_auth("user", "pass")
ctx.set_ssl_certificates("client.crt", "client.key")
ctx.set_will("client/status", "offline", qos = 1, retain = true)
ctx.setProtocolVersion(mpv5)

await ctx.start()
await ctx.publish("topic", "payload", qos = 0, retain = false)

await ctx.subscribe("topic/+", 0) do (topic, message: string):
  echo topic, ": ", message

echo ctx.isConnected()
echo ctx.msgQueue()

await ctx.unsubscribe("topic/+")
await ctx.disconnect()
```

### Compatibility notes

`publish()` keeps nmqtt-style queue-oriented behavior. It returns after the
command is accepted by the worker queue, not after PUBACK.

`msgQueue()` tracks unfinished work such as QoS publish completion and
subscribe/unsubscribe acknowledgements.

### Extension API

`publishV5()` is provided as an extension for MQTT v5 properties.

Example:

```nim
await ctx.publishV5(
  topic = "rpc/request",
  message = "payload",
  qos = 1,
  properties = @[
    userProperty("trace-id", "abc123"),
    responseTopic("rpc/response/client1"),
    correlationData(@[0x01'u8, 0x02, 0x03]),
    messageExpiryInterval(60'u32),
    contentType("text/plain"),
    payloadFormatIndicatorUtf8(),
  ],
)
```

## 6. Testing

Normal tests:

```sh
nimble test
```

Broker-gated tests:

```sh
MOSQUITTO_NIM_TEST_BROKER=1 nimble test
```

Optional broker environment variables:

```sh
MOSQUITTO_NIM_TEST_HOST=127.0.0.1
MOSQUITTO_NIM_TEST_PORT=1883
```

TLS broker tests are not yet part of the default test flow.
