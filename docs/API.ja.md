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

### TLS 設定

`MqttTlsConfig` は、よく使う TLS 設定を分けて扱います。

- `mqttTlsWithOsTrustStore()` は公開 CA ベースのクラウド broker 向け。
- `mqttTlsWithCa(cafile)` / `mqttTlsWithCaPath(capath)` はプライベート CA 向け。
- `mqttTlsClientCertificate(certfile, keyfile, ...)` は mTLS client certificate 認証向け。
- `insecure = true` / `setTlsInsecure()` は開発・ローカル broker 検証用。

有効な TLS 設定なのに CA source も OS trust store もない場合や、client cert/key の片方だけが指定された場合は validation error になります。

highlevel client では、今後の connect に使う TLS 設定を保存できます。

```nim
discard client.setTlsWithOsTrustStore()
discard client.setTlsClientCertificate("device.crt", "device.key")
discard client.setTlsCa("private-ca.crt")
discard client.setTlsInsecure(true)
discard client.clearTls()
```

nmqtt 互換 facade には同等の helper があります。

```nim
ctx.set_host("example.azure-devices.net", 8883, sslOn = true)
discard ctx.set_tls_os_certs()
discard ctx.set_tls_ca("private-ca.crt")
ctx.set_ssl_certificates("device.crt", "device.key")
discard ctx.set_tls_insecure(true)
```

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

この module は、元の nmqtt API を完全に再実装するためのものではなく、**nmqtt に近い書き味と既定挙動を保ちながら、mosquitto_nim 独自の実用 API を追加する facade** です。内部実装は `libmosquitto + threadtools + asyncdispatch` です。

### nmqtt 互換 API

元の nmqtt と同じ、または同等の意味で使うことを意図している API です。

```nim
let ctx = newMqttCtx("client-id")

ctx.set_host("127.0.0.1", 1883)
ctx.set_ping_interval(30)
ctx.set_auth("user", "pass")
ctx.set_ssl_certificates("client.crt", "client.key")
ctx.set_will("client/status", "offline", qos = 1, retain = true)

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

`msgQueue()` は QoS publish completion や subscribe/unsubscribe ack、offline queue など、互換 facade で未完了として扱う作業数を返します。

`start()` 後の reconnect と offline publish queue は nmqtt 風の利用感に寄せ、compat facade では既定で有効です。

### mosquitto_nim 拡張 API

以下は nmqtt そのものにはない、mosquitto_nim の拡張 API です。

#### MQTT protocol version

```nim
ctx.setProtocolVersion(mpv5)
```

#### TLS trust source / test configuration

```nim
discard ctx.set_tls_os_certs()
discard ctx.set_tls_ca("ca.crt")
discard ctx.set_tls_capath("/etc/ssl/certs")
ctx.set_tls_insecure(true)  # test only
```

`set_ssl_certificates(certfile, keyfile)` は互換 API として残しつつ、mosquitto_nim では mTLS client certificate / private key 設定として扱います。公開 CA の broker に接続するだけなら `set_tls_os_certs()` や `set_tls_ca()` を使います。

#### MQTT v5 CONNECT metadata

```nim
var connectProps = noConnectProperties()
connectProps.setSessionExpiryInterval(3600'u32)
connectProps.setReceiveMaximum(16'u16)
connectProps.setRequestProblemInformation(true)
discard ctx.setConnectProperties(connectProps)
ctx.setProtocolVersion(mpv5)
```

#### MQTT v5 SUBSCRIBE metadata

```nim
var subProps = noSubscribeProperties()
subProps.setSubscriptionIdentifier(7)
subProps.addUserProperty("route", "telemetry")

await ctx.subscribeV5("telemetry/#", subProps, 1) do (topic, message: string):
  echo topic, ": ", message
```

#### MQTT v5 PUBLISH metadata

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

#### Reconnect / offline queue policy

```nim
discard ctx.enableReconnect(initialDelayMs = 1000, maxDelayMs = 30000)
discard ctx.enableOfflineQueue(maxMessages = 100, maxBytes = 1024 * 1024)
```

#### 状態・診断情報

```nim
echo ctx.currentState()
echo ctx.pendingOperations()
echo ctx.queueSnapshot()
echo ctx.lastConnectReasonCode()
echo ctx.lastConnectProperties()
```

互換 API と拡張 API の一覧は、今後 `docs/NMQTT_COMPAT.ja.md` のような compatibility matrix として分離する予定です。

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
