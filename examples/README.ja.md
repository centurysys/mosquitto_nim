# mosquitto_nim examples

このディレクトリには、`mosquitto_nim` の利用例を置きます。

大きく分けて、次の 2 種類があります。

- highlevel API examples
- nmqtt-compatible facade examples

`nmqtt_compat` は、元の nmqtt に近い callback-oriented API と queue-oriented な `publish()` の意味を維持しつつ、`mosquitto_nim` 独自拡張として MQTT v5 properties / reconnect / offline queue / TLS などを追加した facade です。

## 共通の実行方法

ローカルで Mosquitto が `127.0.0.1:1883` で動いている前提なら、次のように実行できます。

```bash
nim c -r --path:src examples/highlevel_pubsub.nim
nim c -r --path:src examples/highlevel_dispatcher.nim
nim c -r --path:src examples/highlevel_mqttv5_properties.nim

nim c -r --path:src examples/nmqtt_compat_pubsub.nim
nim c -r --path:src examples/nmqtt_compat_reconnect_offline_queue.nim
nim c -r --path:src examples/nmqtt_compat_mqttv5_properties.nim
```

broker の host / port / topic は環境変数で変えられます。

```bash
MQTT_HOST=127.0.0.1 \
MQTT_PORT=1883 \
MQTT_TOPIC=mosquitto_nim/examples/test \
nim c -r --path:src examples/highlevel_pubsub.nim
```

## highlevel API examples

### examples/highlevel_pubsub.nim

`nextEvent()` を直接読む例です。

- `startMqttClient()` で worker thread と async bridge を起動
- connect command を投入
- `mevConnected` を待つ
- subscribe command を投入
- `mevSubscribed` を待つ
- publish command を投入
- `mevMessageReceived` と `mevPublishCompleted` を待つ

highlevel のイベントストリームを理解するための一番素直な例です。

### examples/highlevel_dispatcher.nim

handler 登録型の例です。

- `subscribe(topic, qos, handler)` で handler 登録と SUBSCRIBE command 投入をまとめる
- event loop では `nextEvent()` で受けた event を `dispatchEvent()` に渡す
- `mevMessageReceived` のときだけ handler が呼ばれる

アプリケーション側で topic filter ごとに処理を分けたい場合はこちらの使い方が近いです。

### examples/highlevel_mqttv5_properties.nim

highlevel API で typed MQTT v5 properties を使う例です。

- `MqttConnectProperties`
- `MqttSubscribeProperties`
- `MqttPublishProperties`
- CONNACK / received PUBLISH properties の表示

MQTT v5 対応 broker で実行してください。Mosquitto 2.x なら通常のローカル broker で試せます。

## nmqtt-compatible facade examples

### examples/nmqtt_compat_pubsub.nim

nmqtt 風 API の最小 pub/sub 例です。

- `newMqttCtx()` で context 作成
- `set_host()` / `set_ping_interval()` で設定
- `start()` で接続と event pump を開始
- `subscribe(topic, qos, callback)` で callback 登録
- `publish()` は worker queue に投入できた時点で戻る
- `msgQueue()` で pending / offline queue の状態を確認

元 nmqtt に近い使い方を確認するための例です。

### examples/nmqtt_compat_reconnect_offline_queue.nim

reconnect と offline publish queue の動作を見る例です。

`nmqtt_compat` では、元 nmqtt の workQueue 風の使い勝手に寄せて、reconnect と bounded offline queue が既定で有効です。この例では明示的にも設定し、定期的に publish しながら queue snapshot を表示します。

実行中に broker を停止・再起動すると、offline queue / reconnect の動きが見えます。

```bash
MQTT_MESSAGES=20 \
MQTT_INTERVAL_MS=1000 \
nim c -r --path:src examples/nmqtt_compat_reconnect_offline_queue.nim
```

### examples/nmqtt_compat_mqttv5_properties.nim

nmqtt-compatible facade から mosquitto_nim 拡張の MQTT v5 properties を使う例です。

- `setProtocolVersion(mpv5)`
- `setConnectProperties()`
- `subscribeV5()`
- `publishV5()`
- `lastConnectReasonCode()`
- `lastConnectProperties()`

元 nmqtt API にはない mosquitto_nim 拡張 API の使い方を確認するための例です。

### examples/nmqtt_compat_tls_mtls.nim

nmqtt-compatible facade で TLS / mTLS 接続を行う例です。

通常 TLS の例:

```bash
MQTT_HOST=localhost \
MQTT_PORT=8883 \
MQTT_TLS_CAFILE="$HOME/tmp/mosquitto-tls-test/ca.crt" \
nim c -r --path:src examples/nmqtt_compat_tls_mtls.nim
```

mTLS の例:

```bash
MQTT_HOST=localhost \
MQTT_PORT=8884 \
MQTT_TLS_CAFILE="$HOME/tmp/mosquitto-tls-test/ca.crt" \
MQTT_TLS_CERTFILE="$HOME/tmp/mosquitto-tls-test/client.crt" \
MQTT_TLS_KEYFILE="$HOME/tmp/mosquitto-tls-test/client.key" \
nim c -r --path:src examples/nmqtt_compat_tls_mtls.nim
```

hostname verification を一時的に無効化したい開発用途では、明示的に `MQTT_TLS_INSECURE=1` を指定します。

```bash
MQTT_TLS_INSECURE=1 \
nim c -r --path:src examples/nmqtt_compat_tls_mtls.nim
```

MQTT v3.1.1 で確認したい場合は `MQTT_V5=0` を指定します。Azure IoT Hub の device endpoint を意識した確認では、MQTT v3.1.1 側の経路も重要です。
