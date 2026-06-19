# Destination: examples/nmqtt_compat_mqttv5_properties.nim

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

proc printProperties(properties: MqttProperties; prefix = "  ") =
  if properties.len == 0:
    echo prefix, "properties: none"
    return

  echo prefix, "properties:"
  for property in properties:
    case property.kind
    of mpUserProperty:
      echo prefix, &"  User Property: {property.name}={property.value}"
    of mpCorrelationData:
      echo prefix, &"  Correlation Data: {property.data.len} bytes"
    of mpSubscriptionIdentifier:
      echo prefix, &"  Subscription Identifier: {property.intValue}"
    else:
      echo prefix, &"  {property.kind}: {propertyDataString(property)}"

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
  let topic = getEnv("MQTT_TOPIC", "mosquitto_nim/examples/nmqtt/v5")
  let clientId = getEnv(
    "MQTT_CLIENT_ID",
    &"mosquitto_nim_nmqtt_v5_{getCurrentProcessId()}"
  )

  let ctx = newMqttCtx(clientId)
  ctx.set_host(host, port)
  ctx.set_ping_interval(30)
  ctx.setProtocolVersion(mpv5)

  var connectProps = mqttConnectProperties()
  connectProps.setSessionExpiryInterval(60)
  connectProps.setReceiveMaximum(16)
  connectProps.setRequestProblemInformation(true)
  connectProps.addUserProperty("example", "nmqtt_compat_mqttv5_properties")
  discard unwrap(ctx.setConnectProperties(connectProps), "set CONNECT properties")

  var received = false
  var receivedPayload = ""

  try:
    await ctx.start()
    echo &"connected with MQTT v5: reasonCode={ctx.lastConnectReasonCode()}"
    printProperties(ctx.lastConnectProperties())

    var subscribeProps = mqttSubscribeProperties(1001)
    subscribeProps.addUserProperty("purpose", "example-subscribe")

    await ctx.subscribeV5(topic, properties = subscribeProps, qos = 1,
      callback = proc(topic: string; message: string) =
        received = true
        receivedPayload = message
        echo &"callback: topic={topic} payload={message}"
    )
    await waitForQueueEmpty(ctx, "SUBACK")

    var publishProps = noPublishProperties()
    publishProps.addUserProperty("trace-id", &"{epochTime()}")
    publishProps.setContentType("text/plain")
    publishProps.setPayloadFormatIndicator(mpfiUtf8)
    publishProps.setMessageExpiryInterval(30)

    let payload = "hello from nmqtt_compat_mqttv5_properties"
    await ctx.publishV5(topic, payload, properties = publishProps, qos = 1)

    let deadline = epochTime() + 5.0
    while not received and epochTime() < deadline:
      await sleepAsync(20)

    if not received:
      fail("message callback was not called")
    if receivedPayload != payload:
      fail(&"payload mismatch: expected={payload} actual={receivedPayload}")

    await waitForQueueEmpty(ctx, "QoS1 publish completion")
    echo "done"
  except MqttCompatError as e:
    fail(&"MQTT error: {e.msg}")
  finally:
    await ctx.disconnect()

when isMainModule:
  waitFor main()
