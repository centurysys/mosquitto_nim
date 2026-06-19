# Destination: examples/nmqtt_compat_pubsub.nim

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
  let host = getEnv("MQTT_HOST", "127.0.0.1")
  let port = envInt("MQTT_PORT", 1883)
  let topic = getEnv("MQTT_TOPIC", "mosquitto_nim/examples/nmqtt/pubsub")
  let clientId = getEnv(
    "MQTT_CLIENT_ID",
    &"mosquitto_nim_nmqtt_pubsub_{getCurrentProcessId()}"
  )
  let payload = &"hello from nmqtt_compat_pubsub at {epochTime()}"

  let ctx = newMqttCtx(clientId)
  ctx.set_host(host, port)
  ctx.set_ping_interval(30)

  var received = false
  var receivedPayload = ""

  try:
    await ctx.start()
    echo &"connected: host={host} port={port} clientId={clientId}"

    await ctx.subscribe(topic, 1, proc(topic: string; message: string) =
      received = true
      receivedPayload = message
      echo &"callback: topic={topic} payload={message}"
    )
    await waitForQueueEmpty(ctx, "SUBACK")
    echo &"subscribed: topic={topic}"

    await ctx.publish(topic, payload, qos = 1)
    echo &"publish accepted locally: msgQueue={ctx.msgQueue()}"

    let deadline = epochTime() + 5.0
    while not received and epochTime() < deadline:
      await sleepAsync(20)

    if not received:
      fail("message callback was not called")
    if receivedPayload != payload:
      fail(&"payload mismatch: expected={payload} actual={receivedPayload}")

    await waitForQueueEmpty(ctx, "QoS1 publish completion")
    echo &"done: msgQueue={ctx.msgQueue()}"
  except MqttCompatError as e:
    fail(&"MQTT error: {e.msg}")
  finally:
    await ctx.disconnect()

when isMainModule:
  waitFor main()
