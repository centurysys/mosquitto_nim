# Destination: examples/highlevel_mqttv5_properties.nim

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

proc printProperties(properties: MqttProperties; prefix = "  ") =
  if properties.len == 0:
    echo prefix, "properties: none"
    return

  echo prefix, "properties:"
  for property in properties:
    case property.kind
    of mpUserProperty:
      echo prefix, "  User Property: ", property.name, "=", property.value
    of mpCorrelationData:
      echo prefix, "  Correlation Data: ", property.data.len, " bytes"
    of mpSubscriptionIdentifier:
      echo prefix, "  Subscription Identifier: ", property.intValue
    else:
      echo prefix, "  ", property.kind, ": ", propertyDataString(property)

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
  let topic = getEnv("MQTT_TOPIC", "mosquitto_nim/examples/highlevel/v5")
  let clientId = getEnv("MQTT_CLIENT_ID", "mosquitto_nim_highlevel_v5")

  let client = unwrap(startMqttClient(clientId = clientId), "start client")

  try:
    var connectProps = mqttConnectProperties()
    connectProps.setSessionExpiryInterval(60)
    connectProps.setReceiveMaximum(16)
    connectProps.setRequestProblemInformation(true)
    connectProps.addUserProperty("example", "highlevel_mqttv5_properties")

    discard unwrap(client.setConnectProperties(connectProps), "set CONNECT properties")

    discard unwrap(
      client.connect(
        host,
        port = port,
        keepalive = 30,
        protocolVersion = mpv5
      ),
      "queue MQTT v5 connect"
    )

    while true:
      let event = unwrap(await client.nextEvent(), "await connect event")
      case event.kind
      of mevConnected:
        echo "connected with MQTT v5: reasonCode=", event.reasonCode
        printProperties(event.properties)
        break
      of mevError:
        fail("connect failed: " & $event.error)
      else:
        discard

    var subscribeProps = mqttSubscribeProperties(42)
    subscribeProps.addUserProperty("purpose", "example-subscribe")

    let sub = unwrap(
      client.subscribeV5(topic, properties = subscribeProps, qos = qos1),
      "queue MQTT v5 subscribe"
    )

    while true:
      let event = unwrap(await client.nextEvent(), "await subscribe event")
      case event.kind
      of mevSubscribed:
        if event.commandId == sub:
          echo "subscribed with MQTT v5 properties"
          printProperties(event.properties)
          break
      of mevError:
        if event.commandId == sub:
          fail("subscribe failed: " & $event.error)
      else:
        discard

    var publishProps = noPublishProperties()
    publishProps.addUserProperty("trace-id", $epochTime())
    publishProps.setContentType("text/plain")
    publishProps.setPayloadFormatIndicator(mpfiUtf8)
    publishProps.setMessageExpiryInterval(30)

    let payload = "hello with MQTT v5 properties"
    discard unwrap(
      client.publishV5(topic, payload, properties = publishProps, qos = qos1),
      "queue MQTT v5 publish"
    )

    while true:
      let event = unwrap(await client.nextEvent(), "await MQTT v5 message")
      case event.kind
      of mevMessageReceived:
        echo "message received: topic=", event.message.topic
        echo "payload: ", payloadString(event.message)
        printProperties(event.message.properties)
        break
      of mevError:
        fail("worker error: " & $event.error)
      else:
        discard

    discard unwrap(client.disconnect(), "queue disconnect")
  finally:
    await stopClient(client)

when isMainModule:
  waitFor main()
