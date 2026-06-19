# Destination: examples/nmqtt_compat_tls_mtls.nim

import std/[asyncdispatch, os, strformat, strutils, times]

import results

import mosquitto_nim

proc envInt(name: string; defaultValue: int): int =
  let value = getEnv(name)
  if value.len == 0:
    return defaultValue

  try:
    result = parseInt(value)
  except ValueError:
    stderr.writeLine(&"Invalid integer in {name}: {value}")
    quit 1

proc envBool(name: string; defaultValue = false): bool =
  let value = getEnv(name).toLowerAscii()
  if value.len == 0:
    return defaultValue
  result = value in ["1", "true", "yes", "on"]

proc fail(message: string) =
  stderr.writeLine(message)
  quit 1

proc unwrap[T](res: MqttResult[T]; context: string): T =
  if res.isErr:
    fail(&"{context}: {res.error}")
  result = res.get()

proc waitForQueueEmpty(ctx: MqttCtx; label: string; timeoutMs = 5000) {.async.} =
  let deadline = epochTime() + timeoutMs.float / 1000.0
  while epochTime() < deadline:
    if ctx.msgQueue() == 0:
      return
    await sleepAsync(20)

  fail(&"timeout while waiting for {label}; msgQueue={ctx.msgQueue()}")

proc main() {.async.} =
  let host = getEnv("MQTT_HOST", "localhost")
  let port = envInt("MQTT_PORT", 8883)
  let cafile = getEnv("MQTT_TLS_CAFILE")
  let certfile = getEnv("MQTT_TLS_CERTFILE")
  let keyfile = getEnv("MQTT_TLS_KEYFILE")
  let insecure = envBool("MQTT_TLS_INSECURE")
  let useMqttV5 = envBool("MQTT_V5", true)
  let topic = getEnv("MQTT_TOPIC", "mosquitto_nim/examples/nmqtt/tls")
  let clientId = getEnv(
    "MQTT_CLIENT_ID",
    &"mosquitto_nim_nmqtt_tls_{getCurrentProcessId()}"
  )
  let payload = &"hello from nmqtt_compat_tls_mtls at {epochTime()}"

  let ctx = newMqttCtx(clientId)
  ctx.set_host(host, port, sslOn = true)
  ctx.set_ping_interval(30)

  if useMqttV5:
    ctx.setProtocolVersion(mpv5)
  else:
    ctx.setProtocolVersion(mpv311)

  if cafile.len > 0:
    discard unwrap(ctx.set_tls_ca(cafile), "set TLS CA file")
  else:
    discard unwrap(ctx.set_tls_os_certs(), "set OS trust store TLS")

  if certfile.len > 0 or keyfile.len > 0:
    if certfile.len == 0 or keyfile.len == 0:
      fail("MQTT_TLS_CERTFILE and MQTT_TLS_KEYFILE must be specified together")
    ctx.set_ssl_certificates(certfile, keyfile)

  if insecure:
    discard unwrap(ctx.set_tls_insecure(true), "enable insecure TLS mode")

  let protocolName = if useMqttV5: "MQTT v5" else: "MQTT v3.1.1"

  var received = false

  try:
    await ctx.start()
    echo &"connected: host={host} port={port} protocol={protocolName}"
    echo &"TLS: cafile={cafile} certfile={certfile} insecure={insecure}"

    await ctx.subscribe(topic, 1, proc(topic: string; message: string) =
      received = true
      echo &"callback: topic={topic} payload={message}"
    )
    await waitForQueueEmpty(ctx, "SUBACK")

    await ctx.publish(topic, payload, qos = 1)

    let deadline = epochTime() + 5.0
    while not received and epochTime() < deadline:
      await sleepAsync(20)

    if not received:
      fail("message callback was not called")

    await waitForQueueEmpty(ctx, "QoS1 publish completion")
    echo "done"
  except MqttCompatError as e:
    fail(&"MQTT error: {e.msg}")
  finally:
    await ctx.disconnect()

when isMainModule:
  waitFor main()
