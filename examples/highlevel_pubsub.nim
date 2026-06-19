# Destination: examples/highlevel_pubsub.nim

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

proc waitForConnected(client: MqttClient) {.async.} =
  while true:
    let event = unwrap(await client.nextEvent(), "await connect event")
    case event.kind
    of mevConnected:
      echo "connected: reasonCode=", event.reasonCode
      return
    of mevError:
      fail("connect failed: " & $event.error)
    else:
      discard

proc waitForSubscribed(client: MqttClient; commandId: int) {.async.} =
  while true:
    let event = unwrap(await client.nextEvent(), "await subscribe event")
    case event.kind
    of mevSubscribed:
      if event.commandId == commandId:
        echo "subscribed: mid=", event.mid, " grantedQos=", event.grantedQos
        return
    of mevError:
      if event.commandId == commandId:
        fail("subscribe failed: " & $event.error)
    else:
      discard

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
  let topic = getEnv("MQTT_TOPIC", "mosquitto_nim/examples/highlevel/pubsub")
  let payload = "hello from highlevel_pubsub at " & $epochTime()
  let clientId = getEnv("MQTT_CLIENT_ID", "mosquitto_nim_highlevel_pubsub")

  let client = unwrap(startMqttClient(clientId = clientId), "start client")

  try:
    discard unwrap(client.connect(host, port = port, keepalive = 30), "queue connect")
    await waitForConnected(client)

    let subscribeCommandId = unwrap(client.subscribe(topic, qos = qos1), "queue subscribe")
    await waitForSubscribed(client, subscribeCommandId)

    let publishCommandId = unwrap(client.publish(topic, payload, qos = qos1), "queue publish")
    echo "publish queued: commandId=", publishCommandId

    var gotMessage = false
    var gotPublishCompleted = false

    while not (gotMessage and gotPublishCompleted):
      let event = unwrap(await client.nextEvent(), "await publish/message event")
      case event.kind
      of mevMessageReceived:
        echo "message received:"
        echo "  topic  : ", event.message.topic
        echo "  qos    : ", event.message.qos
        echo "  retain : ", event.message.retain
        echo "  payload: ", payloadString(event.message)
        gotMessage = true
      of mevPublishCompleted:
        if event.commandId == publishCommandId:
          echo "publish completed: mid=", event.mid, " reasonCode=", event.reasonCode
          gotPublishCompleted = true
      of mevError:
        fail("worker error: " & $event.error)
      else:
        discard

    discard unwrap(client.disconnect(), "queue disconnect")
    while true:
      let event = unwrap(await client.nextEvent(), "await disconnect event")
      case event.kind
      of mevDisconnected:
        echo "disconnected"
        break
      of mevError:
        fail("disconnect error: " & $event.error)
      else:
        discard
  finally:
    await stopClient(client)

when isMainModule:
  waitFor main()
