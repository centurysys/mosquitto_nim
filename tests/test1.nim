# Destination: tests/test1.nim

import std/[options, os, strutils, times, unittest]

import results

import mosquitto_nim
import mosquitto_nim/lowlevel/bridge
import mosquitto_nim/lowlevel/bindings/c_api

suite "mosquitto_nim lowlevel smoke test":
  test "libmosquitto version is available":
    let version = libVersion()
    check version.major >= 1
    check libVersionString().len > 0

  test "libmosquitto init and cleanup":
    let initRes = initLibrary()
    check initRes.isOk

    let cleanupRes = cleanupLibrary()
    check cleanupRes.isOk

  test "libmosquitto strerror is available":
    check mqttStrError(0).len > 0

  test "lowlevel client can be created and destroyed":
    check initLibrary().isOk

    let clientRes = newLowLevelClient("mosquitto_nim_step4")
    check clientRes.isOk

    let client = clientRes.get()
    check not client.isClosed
    check closeLowLevelClient(client).isOk
    check client.isClosed

    check cleanupLibrary().isOk

  test "mosquitto message is copied into Nim-owned memory":
    var topic = "mosquitto_nim/test"
    var payload = @[byte(ord('h')), byte(ord('e')), byte(ord('l')), byte(ord('l')), byte(ord('o'))]
    var cmsg = struct_mosquitto_message(
      mid: 42.cint,
      topic: topic.cstring,
      payload: cast[pointer](addr payload[0]),
      payloadlen: payload.len.cint,
      qos: 1.cint,
      retain: true
    )

    let msgRes = copyMessage(addr cmsg)
    check msgRes.isOk

    let msg = msgRes.get()
    check msg.mid == 42
    check msg.topic == "mosquitto_nim/test"
    check msg.payloadString() == "hello"
    check msg.qos == qos1
    check msg.retain
    check not msg.dup

  test "topic validation distinguishes publish topics and subscribe filters":
    check validatePublishTopic("mosquitto_nim/test").isOk
    check validatePublishTopic("mosquitto_nim/+").isErr
    check validatePublishTopic("").isErr

    check validateSubscribeTopic("mosquitto_nim/test").isOk
    check validateSubscribeTopic("mosquitto_nim/+").isOk
    check validateSubscribeTopic("").isErr

  test "message sink can be installed and cleared":
    check initLibrary().isOk

    let clientRes = newLowLevelClient("mosquitto_nim_step4_sink")
    check clientRes.isOk
    let client = clientRes.get()

    var received: seq[MqttMessage] = @[]
    let sink: MessageSink = proc(message: MqttMessage) =
      received.add(message)

    check setMessageSink(client, sink).isOk
    check lastCallbackError(client).isNone
    check clearMessageSink(client).isOk
    check closeLowLevelClient(client).isOk

    check cleanupLibrary().isOk

when getEnv("MOSQUITTO_NIM_TEST_BROKER") == "1":
  suite "mosquitto_nim lowlevel broker smoke test":
    test "manual loop can connect, subscribe, publish, and disconnect":
      let host = getEnv("MOSQUITTO_NIM_TEST_HOST", "127.0.0.1")
      let port = parseInt(getEnv("MOSQUITTO_NIM_TEST_PORT", "1883"))

      check initLibrary().isOk

      let clientRes = newLowLevelClient("mosquitto_nim_step4_broker")
      check clientRes.isOk
      let client = clientRes.get()

      check connectLowLevelClient(client, host, port).isOk
      for _ in 0 ..< 5:
        check loopLowLevelClient(client, timeoutMs = 20).isOk

      let subRes = subscribeLowLevelClient(client, "mosquitto_nim/step4/#", qos1)
      check subRes.isOk

      let pubRes = publishLowLevelClient(client, "mosquitto_nim/step4/hello", "hello", qos1)
      check pubRes.isOk

      for _ in 0 ..< 10:
        check loopLowLevelClient(client, timeoutMs = 20).isOk

      discard disconnectLowLevelClient(client)
      discard closeLowLevelClient(client)
      check cleanupLibrary().isOk

    test "message callback trampoline receives a Nim-owned message":
      let host = getEnv("MOSQUITTO_NIM_TEST_HOST", "127.0.0.1")
      let port = parseInt(getEnv("MOSQUITTO_NIM_TEST_PORT", "1883"))
      let topic = "mosquitto_nim/step4/callback/" & $getTime().toUnix() & "_" & $getCurrentProcessId()

      check initLibrary().isOk

      let clientRes = newLowLevelClient("mosquitto_nim_step4_callback")
      check clientRes.isOk
      let client = clientRes.get()

      var received: seq[MqttMessage] = @[]
      let sink: MessageSink = proc(message: MqttMessage) =
        received.add(message)

      check setMessageSink(client, sink).isOk
      check connectLowLevelClient(client, host, port).isOk

      for _ in 0 ..< 10:
        check loopLowLevelClient(client, timeoutMs = 20).isOk

      let subRes = subscribeLowLevelClient(client, topic, qos1)
      check subRes.isOk

      for _ in 0 ..< 10:
        check loopLowLevelClient(client, timeoutMs = 20).isOk

      let pubRes = publishLowLevelClient(client, topic, "hello-callback", qos1)
      check pubRes.isOk

      for _ in 0 ..< 50:
        check loopLowLevelClient(client, timeoutMs = 20).isOk
        if received.len > 0:
          break

      check lastCallbackError(client).isNone
      check received.len == 1
      if received.len == 1:
        check received[0].topic == topic
        check received[0].payloadString() == "hello-callback"
        check received[0].qos == qos1

      discard disconnectLowLevelClient(client)
      discard closeLowLevelClient(client)
      check cleanupLibrary().isOk
