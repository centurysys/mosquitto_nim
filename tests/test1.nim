# Destination: tests/test1.nim

import std/[asyncdispatch, options, os, strutils, times, unittest]

import results

import mosquitto_nim
import mosquitto_nim/worker/types
import mosquitto_nim/worker/mosquitto_worker
import mosquitto_nim/highlevel/async_bridge
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

  test "message and control sinks can be installed and cleared":
    check initLibrary().isOk

    let clientRes = newLowLevelClient("mosquitto_nim_step4_sink")
    check clientRes.isOk
    let client = clientRes.get()

    var received: seq[MqttMessage] = @[]
    let sink: MessageSink = proc(message: MqttMessage) =
      received.add(message)

    var controls: seq[LowLevelControlEvent] = @[]
    let controlSink: ControlSink = proc(event: LowLevelControlEvent) =
      controls.add(event)

    check setMessageSink(client, sink).isOk
    check setControlSink(client, controlSink).isOk
    check lastCallbackError(client).isNone
    check clearMessageSink(client).isOk
    check clearControlSink(client).isOk
    check closeLowLevelClient(client).isOk

    check cleanupLibrary().isOk



suite "mosquitto_nim worker value types":
  test "worker command constructors keep Nim-owned payload bytes":
    let cmd = publishCommand("mosquitto_nim/worker/test", "hello-worker", qos1, retain = true, id = 7)

    check cmd.kind == mckPublish
    check cmd.id == 7
    check cmd.topic == "mosquitto_nim/worker/test"
    check cmd.payloadString() == "hello-worker"
    check cmd.qos == qos1
    check cmd.retain

  test "worker event constructors keep message data in Nim-owned values":
    var msg = MqttMessage(
      mid: 11,
      topic: "mosquitto_nim/worker/event",
      payload: bytesFromString("event-payload"),
      qos: qos1,
      retain: false,
      dup: false,
      properties: @[]
    )

    let ev = messageReceivedEvent(move msg, commandId = 9)

    check ev.kind == mevMessageReceived
    check ev.commandId == 9
    check ev.message.topic == "mosquitto_nim/worker/event"
    check ev.message.payloadString() == "event-payload"
    check ev.message.qos == qos1

  test "worker command and event summaries are available":
    let cmd = connectCommand("127.0.0.1", port = 1883, keepalive = 30, id = 1)
    let ev = connectedEvent(commandId = 1)
    let accepted = publishAcceptedEvent(mid = 12, commandId = 2)
    let completed = publishCompletedEvent(mid = 12, commandId = 2, reasonCode = 0)

    check cmd.summary().contains("127.0.0.1")
    check ev.summary().contains("commandId=1")
    check accepted.kind == mevPublishAccepted
    check completed.kind == mevPublishCompleted
    check completed.mid == 12


suite "mosquitto_nim worker lifecycle":
  test "worker can start, receive stop command, and emit stopped event":
    let workerRes = startMqttWorker("mosquitto_nim_step6_worker")
    check workerRes.isOk

    if workerRes.isOk:
      let worker = workerRes.get()
      check worker.isStarted
      check worker.requestStop(id = 77).isOk

      var stopped = false
      for _ in 0 ..< 100:
        var event: MqttEvent
        let recvRes = worker.tryReceiveEvent(event)
        check recvRes.isOk

        if recvRes.isOk and recvRes.get():
          if event.kind == mevStopped:
            check event.commandId == 77
            stopped = true
            break

        sleep(10)

      check stopped
      check worker.joinMqttWorker().isOk
      check not worker.isStarted

  test "worker reports invalid connect commands as error events":
    let workerRes = startMqttWorker("mosquitto_nim_step8_worker_error")
    check workerRes.isOk

    if workerRes.isOk:
      let worker = workerRes.get()
      check worker.sendCommand(connectCommand("", id = 91)).isOk
      check worker.requestStop(id = 92).isOk

      var sawError = false
      var sawStopped = false
      for _ in 0 ..< 100:
        var event: MqttEvent
        let recvRes = worker.tryReceiveEvent(event)
        check recvRes.isOk

        if recvRes.isOk and recvRes.get():
          case event.kind
          of mevError:
            if event.commandId == 91:
              sawError = true
              check event.error.kind == meInvalidArgument
          of mevStopped:
            if event.commandId == 92:
              sawStopped = true
          else:
            discard

          if sawError and sawStopped:
            break

        sleep(10)

      check sawError
      check sawStopped
      check worker.joinMqttWorker().isOk


suite "mosquitto_nim highlevel async bridge":
  test "async bridge can await worker stopped event":
    proc scenario(): Future[bool] {.async.} =
      let workerRes = startMqttWorker("mosquitto_nim_step9_async_bridge")
      check workerRes.isOk
      if workerRes.isErr:
        return false

      let worker = workerRes.get()
      let bridgeRes = newMqttAsyncBridge(worker, pollMs = 1)
      check bridgeRes.isOk
      if bridgeRes.isErr:
        discard worker.requestStop(id = 302)
        discard worker.joinMqttWorker()
        return false

      let bridge = bridgeRes.get()
      check worker.requestStop(id = 301).isOk

      var sawStopped = false
      for _ in 0 ..< 100:
        let eventRes = await bridge.nextEvent()
        check eventRes.isOk
        if eventRes.isErr:
          break

        let event = eventRes.get()
        if event.kind == mevStopped and event.commandId == 301:
          sawStopped = true
          break

      bridge.close()
      check bridge.isClosed
      check worker.joinMqttWorker().isOk
      return sawStopped

    check waitFor scenario()

  test "async bridge can drain queued events without waiting":
    let workerRes = startMqttWorker("mosquitto_nim_step9_async_drain")
    check workerRes.isOk

    if workerRes.isOk:
      let worker = workerRes.get()
      let bridgeRes = newMqttAsyncBridge(worker, pollMs = 1)
      check bridgeRes.isOk

      if bridgeRes.isOk:
        let bridge = bridgeRes.get()
        check worker.requestStop(id = 311).isOk

        var sawStopped = false
        for _ in 0 ..< 100:
          let drainRes = bridge.drainEvents()
          check drainRes.isOk
          if drainRes.isOk:
            for event in drainRes.get():
              if event.kind == mevStopped and event.commandId == 311:
                sawStopped = true
                break
          if sawStopped:
            break
          sleep(10)

        check sawStopped
        bridge.close()

      check worker.joinMqttWorker().isOk

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


    test "worker can connect, subscribe, publish, receive, disconnect, and stop":
      let host = getEnv("MOSQUITTO_NIM_TEST_HOST", "127.0.0.1")
      let port = parseInt(getEnv("MOSQUITTO_NIM_TEST_PORT", "1883"))
      let topic = "mosquitto_nim/step8/worker/" & $getTime().toUnix() & "_" & $getCurrentProcessId()

      let workerRes = startMqttWorker("mosquitto_nim_step8_worker", loopTimeoutMs = 10)
      check workerRes.isOk

      if workerRes.isOk:
        let worker = workerRes.get()
        check worker.sendCommand(connectCommand(host, port = port, keepalive = 30, id = 201)).isOk

        var sawConnected = false
        for _ in 0 ..< 200:
          var event: MqttEvent
          let recvRes = worker.tryReceiveEvent(event)
          check recvRes.isOk
          if recvRes.isOk and recvRes.get():
            case event.kind
            of mevConnected:
              if event.commandId == 201:
                sawConnected = true
                break
            of mevError:
              checkpoint event.summary()
            else:
              discard
          sleep(10)
        check sawConnected

        check worker.sendCommand(subscribeCommand(topic, qos1, id = 202)).isOk
        var sawSubscribed = false
        for _ in 0 ..< 200:
          var event: MqttEvent
          let recvRes = worker.tryReceiveEvent(event)
          check recvRes.isOk
          if recvRes.isOk and recvRes.get():
            case event.kind
            of mevSubscribed:
              if event.commandId == 202:
                sawSubscribed = true
                break
            of mevError:
              checkpoint event.summary()
            else:
              discard
          sleep(10)
        check sawSubscribed

        # Give the worker a few loop iterations to flush SUBSCRIBE before PUBLISH.
        sleep(100)

        check worker.sendCommand(publishCommand(topic, "hello-worker", qos1, retain = false, id = 203)).isOk

        var sawPublishAccepted = false
        var sawPublishCompleted = false
        var sawMessage = false
        var acceptedMid = 0
        for _ in 0 ..< 300:
          var event: MqttEvent
          let recvRes = worker.tryReceiveEvent(event)
          check recvRes.isOk
          if recvRes.isOk and recvRes.get():
            case event.kind
            of mevPublishAccepted:
              if event.commandId == 203:
                sawPublishAccepted = true
                acceptedMid = event.mid
            of mevPublishCompleted:
              if event.commandId == 203:
                sawPublishCompleted = true
                if acceptedMid != 0:
                  check event.mid == acceptedMid
            of mevMessageReceived:
              if event.message.topic == topic:
                sawMessage = true
                check event.message.payloadString() == "hello-worker"
                check event.message.qos == qos1
            of mevError:
              checkpoint event.summary()
            else:
              discard

            if sawPublishAccepted and sawPublishCompleted and sawMessage:
              break
          sleep(10)

        check sawPublishAccepted
        check sawPublishCompleted
        check sawMessage

        check worker.sendCommand(disconnectCommand(id = 204)).isOk
        var sawDisconnected = false
        for _ in 0 ..< 100:
          var event: MqttEvent
          let recvRes = worker.tryReceiveEvent(event)
          check recvRes.isOk
          if recvRes.isOk and recvRes.get():
            case event.kind
            of mevDisconnected:
              if event.commandId == 204:
                sawDisconnected = true
                break
            of mevError:
              checkpoint event.summary()
            else:
              discard
          sleep(10)
        check sawDisconnected

        check worker.requestStop(id = 205).isOk
        var sawStopped = false
        for _ in 0 ..< 100:
          var event: MqttEvent
          let recvRes = worker.tryReceiveEvent(event)
          check recvRes.isOk
          if recvRes.isOk and recvRes.get():
            if event.kind == mevStopped and event.commandId == 205:
              sawStopped = true
              break
          sleep(10)
        check sawStopped
        check worker.joinMqttWorker().isOk
