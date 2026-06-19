# ローカル Mosquitto で MQTTS / mTLS を試す手順

この文書は、`mosquitto_nim` の TLS 関連 API をローカル Mosquitto broker で検証するための準備手順です。

目的は以下です。

- ローカル CA を作成する
- broker 用 server certificate を作成する
- client certificate を作成する
- Mosquitto を TLS listener として起動する
- Mosquitto を mTLS listener として起動する
- `mosquitto_sub` / `mosquitto_pub` で疎通確認する
- `mosquitto_nim` の env-gated TLS test に渡す環境変数を整理する

ここで作る証明書・秘密鍵は **ローカル開発用** です。本番環境では使わないでください。

---

## 1. 前提パッケージ

Debian / Ubuntu 系なら次を入れます。

```bash
sudo apt update
sudo apt install -y mosquitto mosquitto-clients openssl ca-certificates
```

Alpine なら次です。

```bash
sudo apk add mosquitto mosquitto-clients openssl ca-certificates
```

Mosquitto は MQTT 5.0 / 3.1.1 / 3.1 を実装しており、TLS listener では `cafile` / `capath` / `certfile` / `keyfile` / `require_certificate` などを使えます。

---

## 2. 作業ディレクトリを作る

リポジトリ外に作る例です。

```bash
mkdir -p ~/tmp/mosquitto-tls-test
cd ~/tmp/mosquitto-tls-test
```

以降のコマンドはこのディレクトリで実行します。

---

## 3. ローカル CA を作る

```bash
openssl genrsa -out ca.key 4096

openssl req -x509 -new -nodes \
  -key ca.key \
  -sha256 \
  -days 3650 \
  -out ca.crt \
  -subj "/C=JP/O=mosquitto_nim local test/CN=mosquitto-nim-local-test-ca"

chmod 600 ca.key
```

確認します。

```bash
openssl x509 -in ca.crt -noout -subject -issuer -dates
```

---

## 4. broker 用 server certificate を作る

`localhost` と `127.0.0.1` の両方で検証できるよう、Subject Alternative Name に `DNS:localhost` と `IP:127.0.0.1` を入れます。

```bash
cat > server.ext <<'EOF_SERVER_EXT'
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost,IP:127.0.0.1
EOF_SERVER_EXT
```

秘密鍵と CSR を作ります。

```bash
openssl genrsa -out server.key 2048

openssl req -new \
  -key server.key \
  -out server.csr \
  -subj "/C=JP/O=mosquitto_nim local test/CN=localhost"
```

ローカル CA で server certificate に署名します。

```bash
openssl x509 -req \
  -in server.csr \
  -CA ca.crt \
  -CAkey ca.key \
  -CAcreateserial \
  -out server.crt \
  -days 825 \
  -sha256 \
  -extfile server.ext

chmod 600 server.key
```

確認します。

```bash
openssl verify -CAfile ca.crt server.crt
openssl x509 -in server.crt -noout -subject -issuer -ext subjectAltName
```

---

## 5. client certificate を作る

mTLS、つまり client certificate 認証の確認に使います。

```bash
cat > client.ext <<'EOF_CLIENT_EXT'
basicConstraints = CA:FALSE
keyUsage = digitalSignature
extendedKeyUsage = clientAuth
subjectAltName = DNS:mosquitto-nim-client
EOF_CLIENT_EXT
```

秘密鍵と CSR を作ります。

```bash
openssl genrsa -out client.key 2048

openssl req -new \
  -key client.key \
  -out client.csr \
  -subj "/C=JP/O=mosquitto_nim local test/CN=mosquitto-nim-client"
```

ローカル CA で client certificate に署名します。

```bash
openssl x509 -req \
  -in client.csr \
  -CA ca.crt \
  -CAkey ca.key \
  -CAcreateserial \
  -out client.crt \
  -days 825 \
  -sha256 \
  -extfile client.ext

chmod 600 client.key
```

確認します。

```bash
openssl verify -CAfile ca.crt client.crt
openssl x509 -in client.crt -noout -subject -issuer -ext extendedKeyUsage
```

---

## 6. TLS listener を起動する

client certificate を要求しない TLS listener です。

絶対パスを使うため、まず変数を作ります。

```bash
WORKDIR="$(pwd)"
```

設定ファイルを作ります。

```bash
cat > mosquitto-tls.conf <<EOF_TLS_CONF
listener 8883 127.0.0.1
protocol mqtt

cafile ${WORKDIR}/ca.crt
certfile ${WORKDIR}/server.crt
keyfile ${WORKDIR}/server.key
require_certificate false

allow_anonymous true
persistence false
log_dest stdout
log_type all
EOF_TLS_CONF
```

起動します。

```bash
mosquitto -c mosquitto-tls.conf -v
```

別端末で subscribe します。

```bash
cd ~/tmp/mosquitto-tls-test

mosquitto_sub \
  -h localhost \
  -p 8883 \
  --cafile ca.crt \
  -V mqttv5 \
  -t 'test/#' \
  -d
```

さらに別端末で publish します。

```bash
cd ~/tmp/mosquitto-tls-test

mosquitto_pub \
  -h localhost \
  -p 8883 \
  --cafile ca.crt \
  -V mqttv5 \
  -t 'test/hello' \
  -m 'hello over tls' \
  -d
```

`mosquitto_sub` 側に `hello over tls` が出れば TLS server verification の確認はできています。

---

## 7. mTLS listener を起動する

client certificate を必須にする listener です。

```bash
WORKDIR="$(pwd)"

cat > mosquitto-mtls.conf <<EOF_MTLS_CONF
listener 8884 127.0.0.1
protocol mqtt

cafile ${WORKDIR}/ca.crt
certfile ${WORKDIR}/server.crt
keyfile ${WORKDIR}/server.key
require_certificate true
use_identity_as_username true

allow_anonymous true
persistence false
log_dest stdout
log_type all
EOF_MTLS_CONF
```

起動します。

```bash
mosquitto -c mosquitto-mtls.conf -v
```

別端末で client certificate なしの接続が失敗することを確認します。

```bash
cd ~/tmp/mosquitto-tls-test

mosquitto_sub \
  -h localhost \
  -p 8884 \
  --cafile ca.crt \
  -V mqttv5 \
  -t 'test/#' \
  -d
```

次に client certificate ありで subscribe します。

```bash
cd ~/tmp/mosquitto-tls-test

mosquitto_sub \
  -h localhost \
  -p 8884 \
  --cafile ca.crt \
  --cert client.crt \
  --key client.key \
  -V mqttv5 \
  -t 'test/#' \
  -d
```

さらに別端末で publish します。

```bash
cd ~/tmp/mosquitto-tls-test

mosquitto_pub \
  -h localhost \
  -p 8884 \
  --cafile ca.crt \
  --cert client.crt \
  --key client.key \
  -V mqttv5 \
  -t 'test/mtls' \
  -m 'hello over mtls' \
  -d
```

`mosquitto_sub` 側に `hello over mtls` が出れば mTLS の確認はできています。

---

## 8. mosquitto_nim の TLS broker test 用 env

TLS server verification のみなら次です。

```bash
export MOSQUITTO_NIM_TEST_TLS_BROKER=1
export MOSQUITTO_NIM_TEST_TLS_HOST=localhost
export MOSQUITTO_NIM_TEST_TLS_PORT=8883
export MOSQUITTO_NIM_TEST_TLS_CAFILE="$HOME/tmp/mosquitto-tls-test/ca.crt"
```

mTLS は、通常 TLS listener とは別の env で有効化します。

`require_certificate true` の listener では client certificate が必須になるため、client certificate なしの TLS test と同じ port で走らせると、通常 TLS test が失敗します。そのため、通常 TLS は `MOSQUITTO_NIM_TEST_TLS_BROKER`、mTLS は `MOSQUITTO_NIM_TEST_MTLS_BROKER` で分けています。

```bash
export MOSQUITTO_NIM_TEST_MTLS_BROKER=1
export MOSQUITTO_NIM_TEST_MTLS_HOST=localhost
export MOSQUITTO_NIM_TEST_MTLS_PORT=8884
export MOSQUITTO_NIM_TEST_TLS_CAFILE="$HOME/tmp/mosquitto-tls-test/ca.crt"
export MOSQUITTO_NIM_TEST_TLS_CERTFILE="$HOME/tmp/mosquitto-tls-test/client.crt"
export MOSQUITTO_NIM_TEST_TLS_KEYFILE="$HOME/tmp/mosquitto-tls-test/client.key"
```

通常 TLS と mTLS の両方を同時に確認する場合は、両方を有効化します。

```bash
export MOSQUITTO_NIM_TEST_TLS_BROKER=1
export MOSQUITTO_NIM_TEST_TLS_HOST=localhost
export MOSQUITTO_NIM_TEST_TLS_PORT=8883

export MOSQUITTO_NIM_TEST_MTLS_BROKER=1
export MOSQUITTO_NIM_TEST_MTLS_HOST=localhost
export MOSQUITTO_NIM_TEST_MTLS_PORT=8884

export MOSQUITTO_NIM_TEST_TLS_CAFILE="$HOME/tmp/mosquitto-tls-test/ca.crt"
export MOSQUITTO_NIM_TEST_TLS_CERTFILE="$HOME/tmp/mosquitto-tls-test/client.crt"
export MOSQUITTO_NIM_TEST_TLS_KEYFILE="$HOME/tmp/mosquitto-tls-test/client.key"
```

既定では MQTT v5 で接続します。MQTT v3.1.1 経路を確認したい場合は次を指定します。

```bash
export MOSQUITTO_NIM_TEST_TLS_PROTOCOL=311
```

証明書の SAN と接続 host が一致しない状態を、開発時だけ一時的に許容したい場合は次を指定します。

```bash
export MOSQUITTO_NIM_TEST_TLS_INSECURE=1
```

設定後、通常どおり実行します。

```bash
nimble test
```

---

## 9. OS trust store 経由の確認

`setTlsWithOsTrustStore()` 系の経路を確認したい場合は、ローカル CA を OS trust store に登録します。

Debian / Ubuntu 系の例です。

```bash
sudo cp ~/tmp/mosquitto-tls-test/ca.crt \
  /usr/local/share/ca-certificates/mosquitto-nim-local-test-ca.crt

sudo update-ca-certificates
```

削除する場合は次です。

```bash
sudo rm -f /usr/local/share/ca-certificates/mosquitto-nim-local-test-ca.crt
sudo update-ca-certificates --fresh
```

Alpine の例です。

```bash
sudo cp ~/tmp/mosquitto-tls-test/ca.crt \
  /usr/local/share/ca-certificates/mosquitto-nim-local-test-ca.crt

sudo update-ca-certificates
```

OS trust store 経由のテストでは、接続先 hostname と server certificate の SAN が一致している必要があります。この文書の手順では `localhost` と `127.0.0.1` を入れているため、どちらでも検証できます。

---

## 10. openssl s_client で TLS だけ確認する

MQTT の前に TLS handshake だけを見たい場合は `openssl s_client` が便利です。

TLS server verification の例です。

```bash
openssl s_client \
  -connect localhost:8883 \
  -verify_hostname localhost \
  -CAfile ca.crt
```

mTLS の例です。

```bash
openssl s_client \
  -connect localhost:8884 \
  -verify_hostname localhost \
  -CAfile ca.crt \
  -cert client.crt \
  -key client.key
```

成功時は `Verify return code: 0 (ok)` が出ます。

---

## 11. よくあるエラー

### `certificate verify failed`

主な原因は次です。

- client 側に `--cafile ca.crt` を渡していない
- server certificate の SAN に接続先 hostname / IP が入っていない
- `localhost` 用証明書なのに別 hostname で接続している
- 別の CA で署名した証明書を混ぜている

まず確認します。

```bash
openssl x509 -in server.crt -noout -subject -issuer -ext subjectAltName
openssl verify -CAfile ca.crt server.crt
```

### `tlsv1 alert unknown ca`

broker が client certificate を検証できていない可能性があります。

- broker の `cafile` が client certificate を署名した CA か確認する
- client 側の `--cert` / `--key` が正しいペアか確認する

確認例です。

```bash
openssl verify -CAfile ca.crt client.crt
openssl x509 -in client.crt -noout -subject -issuer
```

### `Error: Unable to load server key file`

Mosquitto process が `server.key` を読めていません。

この文書のように foreground で自分のユーザーとして起動する場合は通常問題になりにくいです。systemd の `mosquitto` service として起動する場合は、`mosquitto` ユーザーが証明書・秘密鍵を読める配置と権限にする必要があります。

### TLS listener に非 TLS client で接続している

`mosquitto_sub -p 8883` に `--cafile` を付け忘れると、TLS listener に平文 MQTT で接続しようとして失敗します。

### `localhost` では成功するが `127.0.0.1` で失敗する

server certificate の SAN に `IP:127.0.0.1` が入っていない可能性があります。

この文書の `server.ext` では以下を入れています。

```text
subjectAltName = DNS:localhost,IP:127.0.0.1
```

---

## 12. セキュリティ注意

- `ca.key`, `server.key`, `client.key` は秘密鍵です。Git に commit しないでください。
- この文書の CA はローカル開発専用です。本番環境では使わないでください。
- `setTlsInsecure()` / `tls_insecure_set` 相当の挙動は開発・切り分け用です。本番環境では使わないでください。
- mTLS で `allow_anonymous true` を使っているのはローカル試験を簡単にするためです。本番では ACL / 認可設計を別途行ってください。

---

## 13. 参考

- Mosquitto TLS manual: https://mosquitto.org/man/mosquitto-tls-7.html
- Mosquitto config manual: https://mosquitto.org/man/mosquitto-conf-5.html
- OpenSSL req manual: https://docs.openssl.org/3.5/man1/openssl-req/
- OpenSSL x509v3 config manual: https://docs.openssl.org/3.6/man5/x509v3_config/
