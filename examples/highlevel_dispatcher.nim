# Destination: examples/highlevel_dispatcher.nim

import std/[asyncdispatch, os, strutils, times]

import results

import mosquitto_nim

proc envInt(name: string; defaultValue: int): int =
  let value = getEnv(name)
  if value.len == 0:
    return defaultValue

  try:
    result = parseInt(value)
  except ValueError:
    stderr.writeLine("Invalid integer in " & name & ": " & value)
    quit 1

proc fail(message: string) =
  stderr.writeLine(message)
  quit 1

proc unwrap[T](res: MqttResult[T]; context: string): T =
  if res.isErr:
    fail(context & ": " & $res.error)
  result = res.get()

proc stopClient(client: MqttClient) {.async.} =
  discard client.requestStop()
  while true:
    let eventRes = await client.nextEvent()
    if eventRes.isErr:
      break
    if eventRes.get().kind == mevStopped:
      break

  discard client.joinMqttClient()

proc main() {.async.} =
  let host = getEnv("MQTT_HOST", "127.0.0.1")
  let port = envInt("MQTT_PORT", 1883)
  let topic = getEnv("MQTT_TOPIC", "mosquitto_nim/examples/highlevel/dispatcher")
  let clientId = getEnv("MQTT_CLIENT_ID", "mosquitto_nim_highlevel_dispatcher")

  let client = unwrap(startMqttClient(clientId = clientId), "start client")

  var received = false
  var receivedPayload = ""

  try:
    discard unwrap(client.connect(host, port = port, keepalive = 30), "queue connect")

    var connected = false
    while not connected:
      let event = unwrap(await client.nextEvent(), "await connect event")
      case event.kind
      of mevConnected:
        connected = true
        echo "connected"
      of mevError:
        fail("connect failed: " & $event.error)
      else:
        discard

    let subscription = unwrap(
      client.subscribe(topic, qos1, proc(message: MqttMessage) =
        received = true
        receivedPayload = payloadString(message)
        echo "handler called: topic=", message.topic, " payload=", receivedPayload
      ),
      "subscribe with handler"
    )

    var subscribed = false
    while not subscribed:
      let event = unwrap(await client.nextEvent(), "await subscribe event")
      case event.kind
      of mevSubscribed:
        if event.commandId == subscription.commandId:
          subscribed = true
          echo "subscribed: handlerId=", subscription.handlerId
      of mevError:
        if event.commandId == subscription.commandId:
          fail("subscribe failed: " & $event.error)
      else:
        discard

      discard unwrap(await client.dispatchEvent(event), "dispatch event")

    let payload = "hello from highlevel_dispatcher at " & $epochTime()
    discard unwrap(client.publish(topic, payload, qos = qos1), "queue publish")

    while not received:
      let event = unwrap(await client.nextEvent(), "await message event")
      case event.kind
      of mevError:
        fail("worker error: " & $event.error)
      else:
        discard

      discard unwrap(await client.dispatchEvent(event), "dispatch event")

    discard unwrap(client.unsubscribe(subscription), "unsubscribe")
    discard unwrap(client.disconnect(), "queue disconnect")
  finally:
    await stopClient(client)

when isMainModule:
  waitFor main()
