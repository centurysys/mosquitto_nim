# mosquitto_nim

`mosquitto_nim` は、Mosquitto MQTT C client library を Nim から使うための wrapper です。

現在の実装は次の層に分けています。

- `lowlevel/`: `libmosquitto` を薄く安全に包む層
- `worker/`: `libmosquitto` handle を所有する threadtools ベースの MQTT worker
- `highlevel/`: asyncdispatch 向け client / dispatcher / nmqtt互換 facade
- `lowlevel/bindings/generated/`: Futhark 生成の raw C binding

主な目的は、raw C pointer、C callback trampoline、`libmosquitto` handle の所有権をアプリケーションコードから隠しつつ、MQTT v3.1.1、MQTT v5、TLS、認証、Will、nmqtt 風の async API を扱えるようにすることです。

## 状態

このパッケージは開発中です。

現時点で実装済みの範囲:

- `libmosquitto.so.1` への dynlib binding
- lowlevel client の作成・破棄
- manual loop による lowlevel connect / disconnect / publish / subscribe / unsubscribe
- 受信 MQTT message の Nim owned copy
- libmosquitto callback trampoline
- threadtools ベースの MQTT worker
- worker event を asyncdispatch 側へ渡す bridge
- highlevel client facade
- highlevel message dispatcher
- 最小 nmqtt 互換 facade
- username/password 認証の設定伝搬
- Will message の設定伝搬
- OS trust store / 明示 CA / mTLS client cert/key / 明示 insecure mode を含む TLS 設定伝搬
- MQTT protocol version 選択
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

## 必要なもの

Nim 依存:

```nim
requires "nim >= 2.2.10"
requires "results >= 0.5.1"
requires "threading >= 0.2.1"
requires "threadtools >= 0.1.0"
```

システム依存:

- Debian/Ubuntu: `libmosquitto-dev`
- Alpine: `mosquitto-dev`

実行時には `libmosquitto.so.1` が見えている必要があります。

## TLS について

MQTT over TLS は Nim の SSL 機能ではなく、`libmosquitto` 側で処理します。  
そのため、`mosquitto_nim` 自体のために Nim を `-d:ssl` 付きでビルドする必要はありません。

ただし、同じアプリケーション内で Nim 標準の `std/httpclient`、`std/net`、`std/asyncnet` などを使って HTTPS/TLS 通信を行う場合、その部分のために `-d:ssl` が必要になることがあります。

現在の dynlib 方式では、`libssl` / `libcrypto` は `libmosquitto.so.1` の動的依存として解決されます。

公開クラウド broker のサーバー証明書は通常、公開 CA で署名されたものです。
オレオレ証明書が必要になるのは、主にローカル・プライベート broker の試験時です。
公開 CA ベースの broker には `mqttTlsWithOsTrustStore()` / `set_tls_os_certs()`、
プライベート CA には `mqttTlsWithCa()` / `set_tls_ca()`、mTLS の client cert/key には
`mqttTlsClientCertificate()` / `set_ssl_certificates()` を使います。
`insecure` / `set_tls_insecure()` は開発・ローカル検証用として明示的に指定する扱いです。

## 簡単な例: nmqtt互換 facade

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

## MQTT v5 の例

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

## nmqtt互換範囲

互換 facade では、nmqtt 風の queue-oriented な `publish()` の意味を維持します。

- `publish()` は publish command が worker command queue に投入できた時点で戻ります。
- PUBACK や `PublishCompleted` は待ちません。
- publish 完了は worker/highlevel event として別途追跡します。

現在サポートしている互換API:

- `newMqttCtx(clientId)`
- `set_host(host, port = 1883, sslOn = false)`
- `set_ping_interval(seconds)`
- `set_auth(username, password)`
- `set_tls_os_certs()` / `set_tls_ca(cafile)` / `set_tls_capath(capath)`
- `set_tls_insecure(insecure = true)`
- `set_ssl_certificates(certfile, keyfile)`
- `set_will(topic, message, qos = 0, retain = false)`
- `setProtocolVersion(mpv311 | mpv5)`
- `start()`
- `connect()`
- `disconnect()`
- `publish(topic, message, qos = 0, retain = false)`
- `publishV5(...)` 独自拡張API
- `subscribe(topic, qos, callback)`
- `subscribeV5(...)` 独自拡張API
- `unsubscribe(topic)`
- `isConnected()`
- `msgQueue()`

未完成・今後の対象:

- nmqtt API完全互換
- MQTT v5 properties の全対応
- reconnect / offline queue の詳細な edge-case 対応
- TLS broker 実接続テスト
- WebSocket / WSS 対応

## 設計概要

`libmosquitto` callback からアプリケーション callback は直接呼びません。

流れは次のようにしています。

```text
libmosquitto callback
  -> C-owned data を Nim-owned value にコピー
  -> worker event queue
  -> asyncdispatch bridge
  -> highlevel dispatcher
  -> application callback
```

これにより、アプリケーション callback は asyncdispatch 側で実行され、`libmosquitto` handle は MQTT worker thread に閉じ込められます。

## ライセンス

MIT。詳細は `LICENSE` を参照してください。
