# Destination: examples/nmqtt_compat_reconnect_offline_queue.nim

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

proc printQueue(ctx: MqttCtx; prefix: string) =
  let queue = ctx.queueSnapshot()
  echo &"{prefix}: state={ctx.currentState()} msgQueue={ctx.msgQueue()} " &
       &"pending={queue.pending.total} offlineQueued={queue.offlineQueued} " &
       &"offlineBytes={queue.offlineBytes}"

proc main() {.async.} =
  let host = getEnv("MQTT_HOST", "127.0.0.1")
  let port = envInt("MQTT_PORT", 1883)
  let topic = getEnv("MQTT_TOPIC", "mosquitto_nim/examples/nmqtt/reconnect")
  let clientId = getEnv(
    "MQTT_CLIENT_ID",
    &"mosquitto_nim_nmqtt_reconnect_{getCurrentProcessId()}"
  )
  let count = envInt("MQTT_MESSAGES", 10)
  let intervalMs = envInt("MQTT_INTERVAL_MS", 1000)

  let ctx = newMqttCtx(clientId)
  ctx.set_host(host, port)
  ctx.set_ping_interval(10)

  discard unwrap(
    ctx.enableReconnect(initialDelayMs = 500, maxDelayMs = 5000, multiplier = 2.0),
    "enable reconnect"
  )
  discard unwrap(
    ctx.enableOfflineQueue(maxMessages = 100, maxBytes = 1024 * 1024, qos0Policy = moqQueue),
    "enable offline queue"
  )

  var receivedCount = 0

  try:
    await ctx.start()
    echo &"connected: host={host} port={port} clientId={clientId}"
    echo "While this example is running, stop and restart the broker to observe reconnect/offline queue behavior."

    await ctx.subscribe(topic, 1, proc(topic: string; message: string) =
      inc receivedCount
      echo &"callback[{receivedCount}]: topic={topic} payload={message}"
    )

    for i in 0 ..< count:
      let payload = &"offline-queue message {i + 1}/{count} at {epochTime()}"
      try:
        await ctx.publish(topic, payload, qos = 1)
        echo &"publish accepted locally: {payload}"
      except MqttCompatError as e:
        echo &"publish raised MQTT error: {e.msg}"

      printQueue(ctx, &"after publish {i + 1}")
      await sleepAsync(intervalMs)

    let drainDeadline = epochTime() + 10.0
    while ctx.msgQueue() > 0 and epochTime() < drainDeadline:
      printQueue(ctx, "draining")
      await sleepAsync(500)

    printQueue(ctx, "final")
    echo &"receivedCount={receivedCount}"
  except MqttCompatError as e:
    fail(&"MQTT error: {e.msg}")
  finally:
    await ctx.disconnect()

when isMainModule:
  waitFor main()
