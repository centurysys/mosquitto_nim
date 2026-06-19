# mosquitto_nim highlevel examples

このディレクトリには、`mosquitto_nim` の highlevel API を使う最小例を置きます。

`nmqtt_compat` は元の nmqtt に近い facade ですが、highlevel API は次の構造をそのまま使います。

- `startMqttClient()` で worker thread と async bridge を起動する
- `connect()` / `subscribe()` / `publish()` は worker command queue へ投入する
- broker からの CONNACK / SUBACK / PUBLISH / PUBACK などは `nextEvent()` で受ける
- message callback が欲しい場合は `subscribe(..., handler)` と `dispatchEvent()` を組み合わせる
- 終了時は `requestStop()` のあと `mevStopped` を受け、`joinMqttClient()` する

## 共通の実行方法

ローカルで Mosquitto が `127.0.0.1:1883` で動いている前提なら、次のように実行できます。

```bash
nim c -r --path:src examples/highlevel_pubsub.nim
nim c -r --path:src examples/highlevel_dispatcher.nim
nim c -r --path:src examples/highlevel_mqttv5_properties.nim
```

broker の host / port / topic は環境変数で変えられます。

```bash
MQTT_HOST=127.0.0.1 \
MQTT_PORT=1883 \
MQTT_TOPIC=mosquitto_nim/examples/test \
nim c -r --path:src examples/highlevel_pubsub.nim
```

## examples/highlevel_pubsub.nim

`nextEvent()` を直接読む例です。

- connect command を投入
- `mevConnected` を待つ
- subscribe command を投入
- `mevSubscribed` を待つ
- publish command を投入
- `mevMessageReceived` と `mevPublishCompleted` を待つ

highlevel のイベントストリームを理解するための一番素直な例です。

## examples/highlevel_dispatcher.nim

handler 登録型の例です。

- `subscribe(topic, qos, handler)` で handler 登録と SUBSCRIBE command 投入をまとめる
- event loop では `nextEvent()` で受けた event を `dispatchEvent()` に渡す
- `mevMessageReceived` のときだけ handler が呼ばれる

アプリケーション側で topic filter ごとに処理を分けたい場合はこちらの使い方が近いです。

## examples/highlevel_mqttv5_properties.nim

highlevel API で typed MQTT v5 properties を使う例です。

- `MqttConnectProperties`
- `MqttSubscribeProperties`
- `MqttPublishProperties`
- CONNACK / received PUBLISH properties の表示

MQTT v5 対応 broker で実行してください。Mosquitto 2.x なら通常のローカル broker で試せます。
