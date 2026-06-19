# mosquitto_nim APIガイド

この文書は、現時点の `mosquitto_nim` の公開API構造を説明します。

ライブラリは意図的に層を分けています。

```text
lowlevel/
  raw libmosquitto wrapper と C/Nim 変換

worker/
  LowLevelClient を所有する threadtools ベースの worker

highlevel/
  asyncdispatch bridge, client facade, dispatcher, nmqtt互換層
```

## 1. Lowlevel API

lowlevel API は `libmosquitto` を包み、呼び出し側には raw C pointer を見せません。

主なモジュール:

- `mosquitto_nim/lowlevel/types`
- `mosquitto_nim/lowlevel/errors`
- `mosquitto_nim/lowlevel/client`
- `mosquitto_nim/lowlevel/bridge`

### Result型

lowlevel 操作は `MqttResult[T]` を返します。

概念的には次の形です。

```nim
type
  MqttResult[T] = Result[T, MqttError]
  MqttOk = object
```

thread境界や callback境界をまたいで例外を飛ばさず、エラーを値として扱います。

### 主な値型

公開される主な lowlevel 型:

- `MqttMessage`
- `MqttQos`
- `MqttProtocolVersion`
- `MqttWill`
- `MqttTlsConfig`
- `MqttProperty`
- `MqttProperties`

`MqttMessage` は topic、payload、properties を Nim owned data として保持します。`libmosquitto` 内部の pointer は外へ出しません。

### Protocol version

対応している protocol version:

- `mpv311`
- `mpv5`

既定は `mpv311` です。

### MQTT v5 properties

CONNECT で対応しているもの:

- `MqttConnectProperties`
- `setSessionExpiryInterval(seconds)`
- `setReceiveMaximum(maximum)`
- `setMaximumPacketSize(bytes)`
- `setRequestProblemInformation(enabled)`
- `addUserProperty(name, value)`

PUBLISH で対応しているもの:

- `userProperty(name, value)`
- `responseTopic(topic)`
- `correlationData(data)`
- `messageExpiryInterval(seconds)`
- `contentType(value)`
- `payloadFormatIndicatorUtf8()`
- `payloadFormatIndicatorUnspecified()`

MQTT v5 CONNACK/control callback からは、libmosquitto が渡した場合に次の properties を Nim-owned value としてコピーします。

- `assignedClientIdentifier(clientId)`
- `serverKeepAlive(seconds)`
- `receiveMaximum(maximum)`
- `maximumPacketSize(bytes)`
- `reasonString(value)`
- `responseInformation(value)`
- `serverReference(value)`
- `userProperty(name, value)`

SUBSCRIBE で現在サポートしているもの:

- `MqttSubscribeProperties`
- `setSubscriptionIdentifier(identifier)`
- `addUserProperty(name, value)`

broker が Subscription Identifier を付与した場合、受信 PUBLISH message 側にも `subscriptionIdentifier(identifier)` が入ります。

これらは `mevConnected`, `mevDisconnected`, `mevPublishCompleted`, `mevSubscribed`, `mevUnsubscribed`, 接続拒否時の `mevError` の `MqttEvent.properties` で参照できます。highlevel / nmqtt 互換 client には直近の CONNACK 系情報を見る `lastConnectReasonCode()` と `lastConnectProperties()` もあります。

新規の PUBLISH コードでは、これらの helper を `mqttPublishProperties(...)` で `MqttPublishProperties` に変換してから `publishV5()` に渡します。新規の SUBSCRIBE コードでは `MqttSubscribeProperties` と `subscribeV5()` を使います。互換性のため従来の generic `MqttProperties` overload も残しています。

API分離用に追加した型付きコンテナ:

- `MqttConnectProperties`
- `MqttPublishProperties`
- `MqttSubscribeProperties`

`MqttConnectProperties` は MQTT v5 CONNECT に接続済みです。`MqttPublishProperties` は publish API に接続済みです。`MqttSubscribeProperties` は `subscribeV5()` API に接続済みです。

## 2. Worker API

worker層は lowlevel client を所有し、worker thread 内で `mosquitto_loop()` を回します。

アプリ側は `MqttCommand` を送り、`MqttEvent` を受け取ります。

### Commands

現在の command 種別:

- Connect
- Disconnect
- Publish
- Subscribe
- Unsubscribe
- Stop

### Events

現在の event 種別:

- Connected
- Disconnected
- PublishAccepted
- PublishCompleted
- Subscribed
- Unsubscribed
- MessageReceived
- Error
- Stopped

### Publish event の意味

publish系eventは意図的に分けています。

```text
PublishAccepted
  mosquitto_publish が message を受け付け、mid を返した

PublishCompleted
  libmosquitto の on_publish callback が呼ばれた
  QoS 1 では PUBACK 相当
  QoS 2 では QoS 2 flow 完了相当
```

この分離は nmqtt 互換のために重要です。nmqtt 風の `publish()` は PUBACK を待つべきではありません。

## 3. Highlevel client API

highlevel client は次を所有します。

- `MqttWorker`
- `MqttAsyncBridge`
- `MqttDispatcher`

アプリケーションは threadtools queue を直接触らず、highlevel client 経由で command を送り、event を await できます。

主なモジュール:

```nim
import mosquitto_nim/highlevel/client
```

主な操作:

- `newMqttClient(...)`
- `connect(...)`
- `disconnect(...)`
- `publish(...)`
- `publishV5(...)`
- `subscribe(...)`
- `subscribeV5(...)`
- `unsubscribe(...)`
- `nextEvent(...)`
- `drainEvents(...)`
- `dispatchNextEvent(...)`
- `dispatchDrainedEvents(...)`
- `requestStop(...)`
- `joinMqttClient(...)`

### Queue-oriented publish

`publish()` は command が worker command queue に投入された時点で戻ります。`PublishCompleted` は待ちません。

送信完了を追跡したい場合は event を見ます。

## 4. Dispatcher API

dispatcher は MQTT topic filter と message handler を対応付けます。

主なモジュール:

```nim
import mosquitto_nim/highlevel/dispatcher
```

対応する handler:

```nim
proc(msg: MqttMessage)
proc(msg: MqttMessage): Future[void]
```

async handler は既定では直列に `await` します。これにより、明示的に並列化しない限りアプリ側 callback が同時実行されにくくなります。

topic filter は通常の MQTT subscription filter を扱います。

- `sensor/+/value`
- `device/#`

## 5. nmqtt互換 facade

主なモジュール:

```nim
import mosquitto_nim/highlevel/nmqtt_compat
```

目的は、nmqtt に近い書き味を保ちながら、内部実装を `libmosquitto + threadtools + asyncdispatch` にすることです。

### 対応API

現時点で対応している範囲:

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

### 互換上の注意

`publish()` は nmqtt 風の queue-oriented な意味を維持します。worker queue に command を投入できた時点で戻り、PUBACK は待ちません。

`msgQueue()` は QoS publish completion や subscribe/unsubscribe ack など、未完了の作業数を追跡します。

### 拡張API

MQTT v5 CONNECT metadata 用に `setConnectProperties()`、PUBLISH metadata 用に `publishV5()`、SUBSCRIBE metadata 用に `subscribeV5()` を追加しています。

CONNECT 例:

```nim
var connectProps = noConnectProperties()
connectProps.setSessionExpiryInterval(3600'u32)
connectProps.setReceiveMaximum(16'u16)
connectProps.setRequestProblemInformation(true)
discard ctx.setConnectProperties(connectProps)
ctx.setProtocolVersion(mpv5)
```

PUBLISH 例:

```nim
var props = noPublishProperties()
props.addUserProperty("trace-id", "abc123")
props.setResponseTopic("rpc/response/client1")
props.setCorrelationData(@[0x01'u8, 0x02, 0x03])
props.setMessageExpiryInterval(60'u32)
props.setContentType("text/plain")
props.setPayloadFormatIndicator(mpfiUtf8)

await ctx.publishV5(
  topic = "rpc/request",
  message = "payload",
  qos = 1,
  properties = props,
)
```

## 6. テスト

通常テスト:

```sh
nimble test
```

brokerを使うテスト:

```sh
MOSQUITTO_NIM_TEST_BROKER=1 nimble test
```

任意の broker 指定:

```sh
MOSQUITTO_NIM_TEST_HOST=127.0.0.1
MOSQUITTO_NIM_TEST_PORT=1883
```

TLS broker 実接続テストは、まだ通常のテストフローには含めていません。
