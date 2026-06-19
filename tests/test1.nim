# Destination: tests/test1.nim

import std/[asyncdispatch, options, os, strutils, times, unittest]

import results

import mosquitto_nim
import mosquitto_nim/worker/types
import mosquitto_nim/worker/mosquitto_worker
import mosquitto_nim/highlevel/async_bridge
import mosquitto_nim/highlevel/client as highlevel_client
import mosquitto_nim/highlevel/dispatcher
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

  test "username/password auth can be configured and cleared":
    check initLibrary().isOk

    let clientRes = newLowLevelClient("mosquitto_nim_step14_auth")
    check clientRes.isOk
    let client = clientRes.get()

    check setUsernamePassword(client, "user1", "pass1").isOk
    check setUsernamePassword(client, "user1", "").isOk
    check setUsernamePassword(client, "", "").isOk
    check setUsernamePassword(client, "", "pass-without-user").isErr

    check closeLowLevelClient(client).isOk
    check cleanupLibrary().isOk


  test "will can be configured and cleared":
    check initLibrary().isOk

    let clientRes = newLowLevelClient("mosquitto_nim_step15_will")
    check clientRes.isOk
    let client = clientRes.get()

    check setWill(client, "mosquitto_nim/will", "offline", qos1, retain = true).isOk
    check clearWill(client).isOk
    check setWill(client, "", "offline", qos0, retain = false).isErr

    check closeLowLevelClient(client).isOk
    check cleanupLibrary().isOk


  test "TLS config can be represented and applied as a no-op when disabled":
    let tls = mqttTls(certfile = "client.crt", keyfile = "client.key")
    check tls.enabled
    check tls.certfile == "client.crt"
    check tls.keyfile == "client.key"

    check initLibrary().isOk

    let clientRes = newLowLevelClient("mosquitto_nim_step16_tls")
    check clientRes.isOk
    let client = clientRes.get()

    # noTls() is intentionally a no-op.  This keeps optional TLS settings easy to
    # apply from worker/highlevel code without special-casing disabled TLS.
    check setTls(client, noTls()).isOk

    check closeLowLevelClient(client).isOk
    check cleanupLibrary().isOk


  test "protocol version can be configured on lowlevel client":
    check initLibrary().isOk

    let clientRes = newLowLevelClient("mosquitto_nim_step17_protocol")
    check clientRes.isOk
    let client = clientRes.get()

    check mpv311.toInt() == 4
    check mpv5.toInt() == 5
    check $mpv311 == "MQTT 3.1.1"
    check $mpv5 == "MQTT 5"
    check setProtocolVersion(client, mpv311).isOk
    check setProtocolVersion(client, mpv5).isOk

    check closeLowLevelClient(client).isOk
    check cleanupLibrary().isOk


suite "mosquitto_nim worker value types":
  test "worker command constructors keep Nim-owned payload bytes":
    let connectCmd = connectCommand(
      "127.0.0.1",
      port = 1883,
      keepalive = 30,
      protocolVersion = mpv5,
      reconnectPolicy = mqttReconnectPolicy(initialDelayMs = 250, maxDelayMs = 4000, multiplier = 1.5),
      offlineQueuePolicy = mqttOfflineQueuePolicy(maxMessages = 25, maxBytes = 4096, qos0Policy = moqDropOldest),
      username = "worker-user",
      password = "worker-pass",
      tls = mqttTls(certfile = "worker.crt", keyfile = "worker.key"),
      id = 6
    )
    check connectCmd.kind == mckConnect
    check connectCmd.username == "worker-user"
    check connectCmd.password == "worker-pass"
    check connectCmd.protocolVersion == mpv5
    check connectCmd.reconnectPolicy.enabled
    check connectCmd.reconnectPolicy.initialDelayMs == 250
    check connectCmd.reconnectPolicy.maxDelayMs == 4000
    check connectCmd.reconnectPolicy.multiplier == 1.5
    check connectCmd.offlineQueuePolicy.enabled
    check connectCmd.offlineQueuePolicy.maxMessages == 25
    check connectCmd.offlineQueuePolicy.maxBytes == 4096
    check connectCmd.offlineQueuePolicy.qos0Policy == moqDropOldest
    check connectCmd.tls.enabled
    check connectCmd.tls.certfile == "worker.crt"
    check connectCmd.summary().contains("auth=true")
    check connectCmd.summary().contains("tls=true")
    check connectCmd.summary().contains("reconnect=true")
    check connectCmd.summary().contains("offlineQueue=true")

    let will = mqttWill("mosquitto_nim/will", "offline", qos1, retain = true)
    let willConnectCmd = connectCommand("127.0.0.1", will = will, id = 8)
    check willConnectCmd.will.enabled
    check willConnectCmd.will.topic == "mosquitto_nim/will"
    check willConnectCmd.will.payload == bytesFromString("offline")
    check willConnectCmd.summary().contains("will=true")

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

  test "worker validates reconnect policy before connect attempts":
    let workerRes = startMqttWorker("mosquitto_nim_step24_worker_reconnect_error")
    check workerRes.isOk

    if workerRes.isOk:
      let worker = workerRes.get()
      let invalidPolicy = MqttReconnectPolicy(
        enabled: true,
        initialDelayMs: 2000,
        maxDelayMs: 1000,
        multiplier: 2.0
      )
      check worker.sendCommand(connectCommand("127.0.0.1", reconnectPolicy = invalidPolicy, id = 93)).isOk
      check worker.requestStop(id = 94).isOk

      var sawError = false
      var sawStopped = false
      for _ in 0 ..< 100:
        var event: MqttEvent
        let recvRes = worker.tryReceiveEvent(event)
        check recvRes.isOk

        if recvRes.isOk and recvRes.get():
          case event.kind
          of mevError:
            if event.commandId == 93:
              sawError = true
              check event.error.kind == meInvalidArgument
              check event.error.message.contains("maxDelayMs")
          of mevStopped:
            if event.commandId == 94:
              sawStopped = true
          else:
            discard

          if sawError and sawStopped:
            break

        sleep(10)

      check sawError
      check sawStopped
      check worker.joinMqttWorker().isOk

  test "worker validates offline queue policy before connect attempts":
    let workerRes = startMqttWorker("mosquitto_nim_step26_worker_offline_queue_error")
    check workerRes.isOk

    if workerRes.isOk:
      let worker = workerRes.get()
      let invalidPolicy = MqttOfflineQueuePolicy(
        enabled: true,
        maxMessages: 0,
        maxBytes: 1024,
        qos0Policy: moqReject
      )
      check worker.sendCommand(connectCommand("127.0.0.1", offlineQueuePolicy = invalidPolicy, id = 95)).isOk
      check worker.requestStop(id = 96).isOk

      var sawError = false
      var sawStopped = false
      for _ in 0 ..< 100:
        var event: MqttEvent
        let recvRes = worker.tryReceiveEvent(event)
        check recvRes.isOk

        if recvRes.isOk and recvRes.get():
          case event.kind
          of mevError:
            if event.commandId == 95:
              sawError = true
              check event.error.kind == meInvalidArgument
              check event.error.message.contains("maxMessages")
          of mevStopped:
            if event.commandId == 96:
              sawStopped = true
          else:
            discard

          if sawError and sawStopped:
            break

        sleep(10)

      check sawError
      check sawStopped
      check worker.joinMqttWorker().isOk

  test "worker queues disconnected publishes when offline queue is enabled":
    let workerRes = startMqttWorker("mosquitto_nim_step26b_worker_offline_queue")
    check workerRes.isOk

    if workerRes.isOk:
      let worker = workerRes.get()
      let reconnectPolicy = mqttReconnectPolicy(initialDelayMs = 60000, maxDelayMs = 60000, multiplier = 1.0)
      let offlinePolicy = mqttOfflineQueuePolicy(maxMessages = 2, maxBytes = 4096, qos0Policy = moqQueue)

      check worker.sendCommand(connectCommand("", reconnectPolicy = reconnectPolicy, offlineQueuePolicy = offlinePolicy, id = 97)).isOk
      check worker.sendCommand(publishCommand("mosquitto_nim/offline/qos1", "queued", qos1, id = 98)).isOk
      check worker.requestStop(id = 99).isOk

      var sawConnectError = false
      var sawQueued = false
      var sawStopped = false
      for _ in 0 ..< 200:
        var event: MqttEvent
        let recvRes = worker.tryReceiveEvent(event)
        check recvRes.isOk

        if recvRes.isOk and recvRes.get():
          case event.kind
          of mevError:
            if event.commandId == 97:
              sawConnectError = true
              check event.error.kind == meInvalidArgument
          of mevQueueChanged:
            if event.commandId == 98 and event.queue.offlineQueued == 1:
              sawQueued = true
              check event.queue.offlineBytes > 0
              check event.queue.total == 1
          of mevStopped:
            if event.commandId == 99:
              sawStopped = true
          else:
            discard

          if sawConnectError and sawQueued and sawStopped:
            break

        sleep(10)

      check sawConnectError
      check sawQueued
      check sawStopped
      check worker.joinMqttWorker().isOk

  test "worker rejects disconnected QoS0 publishes when offline QoS0 policy rejects":
    let workerRes = startMqttWorker("mosquitto_nim_step26b_worker_offline_qos0_reject")
    check workerRes.isOk

    if workerRes.isOk:
      let worker = workerRes.get()
      let reconnectPolicy = mqttReconnectPolicy(initialDelayMs = 60000, maxDelayMs = 60000, multiplier = 1.0)
      let offlinePolicy = mqttOfflineQueuePolicy(maxMessages = 2, maxBytes = 4096, qos0Policy = moqReject)

      check worker.sendCommand(connectCommand("", reconnectPolicy = reconnectPolicy, offlineQueuePolicy = offlinePolicy, id = 100)).isOk
      check worker.sendCommand(publishCommand("mosquitto_nim/offline/qos0", "drop-me", qos0, id = 101)).isOk
      check worker.requestStop(id = 102).isOk

      var sawPublishError = false
      var sawStopped = false
      for _ in 0 ..< 200:
        var event: MqttEvent
        let recvRes = worker.tryReceiveEvent(event)
        check recvRes.isOk

        if recvRes.isOk and recvRes.get():
          case event.kind
          of mevError:
            if event.commandId == 101:
              sawPublishError = true
              check event.error.kind == meInvalidState
              check event.error.message.contains("QoS0")
          of mevStopped:
            if event.commandId == 102:
              sawStopped = true
          else:
            discard

          if sawPublishError and sawStopped:
            break

        sleep(10)

      check sawPublishError
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



suite "mosquitto_nim highlevel client":
  test "highlevel client can start, request stop, await stopped event, and join":
    proc scenario(): Future[bool] {.async.} =
      let clientRes = startMqttClient("mosquitto_nim_step10_client", pollMs = 1)
      check clientRes.isOk
      if clientRes.isErr:
        return false

      let client = clientRes.get()
      check client.isStarted

      let stopIdRes = client.requestStop()
      check stopIdRes.isOk
      if stopIdRes.isErr:
        discard client.joinMqttClient()
        return false

      let stopId = stopIdRes.get()
      var stoppedEvent: Option[MqttEvent] = none(MqttEvent)
      for _ in 0 ..< 200:
        let eventRes = await client.nextEvent()
        check eventRes.isOk
        if eventRes.isErr:
          break

        let event = eventRes.get()
        if event.kind == mevStopped and event.commandId == stopId:
          stoppedEvent = some(event)
          break

      check stoppedEvent.isSome
      check client.currentState() == mcsStopped

      check client.joinMqttClient().isOk
      check client.isClosed
      return stoppedEvent.isSome

    check waitFor scenario()

  test "highlevel client rejects commands after join":
    let clientRes = startMqttClient("mosquitto_nim_step10_closed", pollMs = 1)
    check clientRes.isOk

    if clientRes.isOk:
      let client = clientRes.get()
      let stopIdRes = client.requestStop()
      check stopIdRes.isOk

      if stopIdRes.isOk:
        # Drain the stop event synchronously so join does not race with event use.
        var sawStopped = false
        for _ in 0 ..< 100:
          let drainRes = client.drainEvents()
          check drainRes.isOk
          if drainRes.isOk:
            for event in drainRes.get():
              if event.kind == mevStopped and event.commandId == stopIdRes.get():
                sawStopped = true
                break
          if sawStopped:
            break
          sleep(10)
        check sawStopped

      check client.joinMqttClient().isOk
      let pubRes = client.publish("mosquitto_nim/closed", "nope", qos0)
      check pubRes.isErr



suite "mosquitto_nim highlevel dispatcher":
  test "topic filter matcher follows MQTT wildcard basics":
    check topicFilterMatches("mosquitto_nim/+/value", "mosquitto_nim/device1/value")
    check not topicFilterMatches("mosquitto_nim/+/value", "mosquitto_nim/device1/status")
    check topicFilterMatches("mosquitto_nim/#", "mosquitto_nim")
    check topicFilterMatches("mosquitto_nim/#", "mosquitto_nim/device1/value")
    check not topicFilterMatches("#", "$SYS/broker/version")
    check topicFilterMatches("$SYS/#", "$SYS/broker/version")

  test "dispatcher invokes matching synchronous handlers only":
    let dispatcher = newMqttDispatcher()
    var received: seq[string] = @[]

    let handler: MqttMessageHandler = proc(message: MqttMessage) =
      received.add(message.topic & "=" & message.payloadString())

    let addRes = dispatcher.addMessageHandler("mosquitto_nim/+/dispatch", handler)
    check addRes.isOk
    check dispatcher.handlerCount() == 1

    let missMsg = MqttMessage(
      topic: "mosquitto_nim/device1/other",
      payload: bytesFromString("miss"),
      qos: qos0
    )
    let missCount = waitFor dispatcher.dispatchMessage(missMsg)
    check missCount.isOk
    check missCount.get() == 0
    check received.len == 0

    let hitMsg = MqttMessage(
      topic: "mosquitto_nim/device1/dispatch",
      payload: bytesFromString("hit"),
      qos: qos1
    )
    let hitCount = waitFor dispatcher.dispatchMessage(hitMsg)
    check hitCount.isOk
    check hitCount.get() == 1
    check received == @["mosquitto_nim/device1/dispatch=hit"]

    let removeRes = dispatcher.removeMessageHandler(addRes.get())
    check removeRes.isOk
    check removeRes.get()
    check dispatcher.handlerCount() == 0

  test "dispatcher awaits async handlers sequentially":
    proc scenario(): Future[bool] {.async.} =
      let dispatcher = newMqttDispatcher()
      var trace: seq[string] = @[]

      let asyncHandler: MqttAsyncMessageHandler = proc(message: MqttMessage): Future[void] {.async.} =
        trace.add("begin:" & message.payloadString())
        await sleepAsync(1)
        trace.add("end:" & message.payloadString())

      let addRes = dispatcher.addMessageHandler("mosquitto_nim/async/#", asyncHandler)
      check addRes.isOk
      if addRes.isErr:
        return false

      let msg = MqttMessage(
        topic: "mosquitto_nim/async/dispatch",
        payload: bytesFromString("payload"),
        qos: qos0
      )
      let countRes = await dispatcher.dispatchEvent(messageReceivedEvent(msg))
      check countRes.isOk
      if countRes.isErr:
        return false

      check countRes.get() == 1
      check trace == @["begin:payload", "end:payload"]
      return countRes.get() == 1 and trace == @["begin:payload", "end:payload"]

    check waitFor scenario()



suite "mosquitto_nim highlevel client dispatcher integration":
  test "client-owned dispatcher invokes registered handlers":
    proc scenario(): Future[bool] {.async.} =
      let clientRes = startMqttClient("mosquitto_nim_step12_dispatcher", pollMs = 1)
      check clientRes.isOk
      if clientRes.isErr:
        return false

      let client = clientRes.get()
      var received: seq[string] = @[]

      let handler: MqttMessageHandler = proc(message: MqttMessage) =
        received.add(message.topic & "=" & message.payloadString())

      let handlerRes = client.addMessageHandler("mosquitto_nim/client/+", handler)
      check handlerRes.isOk
      check client.messageHandlerCount() == 1
      if handlerRes.isErr:
        discard client.requestStop()
        discard client.joinMqttClient()
        return false

      let missMsg = MqttMessage(
        topic: "mosquitto_nim/client/other/value",
        payload: bytesFromString("miss"),
        qos: qos0
      )
      let missDispatch = await client.dispatchEvent(messageReceivedEvent(missMsg))
      check missDispatch.isOk
      if missDispatch.isOk:
        check missDispatch.get() == 0
      check received.len == 0

      let hitMsg = MqttMessage(
        topic: "mosquitto_nim/client/value",
        payload: bytesFromString("hit"),
        qos: qos1
      )
      let hitDispatch = await client.dispatchEvent(messageReceivedEvent(hitMsg))
      check hitDispatch.isOk
      if hitDispatch.isOk:
        check hitDispatch.get() == 1
      check received == @[
        "mosquitto_nim/client/value=hit"
      ]

      let removeRes = client.removeMessageHandler(handlerRes.get())
      check removeRes.isOk
      if removeRes.isOk:
        check removeRes.get()
      check client.messageHandlerCount() == 0

      let stopRes = client.requestStop()
      check stopRes.isOk
      if stopRes.isOk:
        discard await client.nextEvent()

      check client.joinMqttClient().isOk
      return received == @["mosquitto_nim/client/value=hit"]

    check waitFor scenario()

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

    test "highlevel client can connect, subscribe, publish, receive, disconnect, and stop":
      proc waitForEvent(client: MqttClient; maxRounds: int;
                        wantedKind: MqttEventKind; commandId = -1;
                        wantedTopic = ""): Future[Option[MqttEvent]] {.async.} =
        for _ in 0 ..< maxRounds:
          let drainRes = client.drainEvents()
          check drainRes.isOk
          if drainRes.isOk:
            for event in drainRes.get():
              if event.kind == wantedKind:
                if commandId >= 0 and event.commandId != commandId:
                  continue
                if wantedTopic.len > 0:
                  if event.kind != mevMessageReceived or event.message.topic != wantedTopic:
                    continue
                return some(event)
          await sleepAsync(10)

        return none(MqttEvent)

      proc scenario(): Future[bool] {.async.} =
        let host = getEnv("MOSQUITTO_NIM_TEST_HOST", "127.0.0.1")
        let port = parseInt(getEnv("MOSQUITTO_NIM_TEST_PORT", "1883"))
        let topic = "mosquitto_nim/step10/highlevel/" & $getTime().toUnix() & "_" & $getCurrentProcessId()

        let clientRes = startMqttClient("mosquitto_nim_step10_highlevel", loopTimeoutMs = 10, pollMs = 1)
        check clientRes.isOk
        if clientRes.isErr:
          return false

        let client = clientRes.get()

        let connectIdRes = client.connect(host, port = port, keepalive = 30)
        check connectIdRes.isOk
        if connectIdRes.isErr:
          discard client.joinMqttClient()
          return false
        let connectEvent = await waitForEvent(client, 300, mevConnected, connectIdRes.get())
        check connectEvent.isSome

        let subscribeIdRes = client.subscribe(topic, qos1)
        check subscribeIdRes.isOk
        if subscribeIdRes.isErr:
          discard client.requestStop()
          discard client.joinMqttClient()
          return false
        let subscribeEvent = await waitForEvent(client, 300, mevSubscribed, subscribeIdRes.get())
        check subscribeEvent.isSome

        let publishIdRes = client.publish(topic, "hello-highlevel", qos1, retain = false)
        check publishIdRes.isOk
        if publishIdRes.isErr:
          discard client.requestStop()
          discard client.joinMqttClient()
          return false

        var sawAccepted = false
        var sawCompleted = false
        var sawMessage = false
        var acceptedMid = 0
        for _ in 0 ..< 400:
          let drainRes = client.drainEvents()
          check drainRes.isOk
          if drainRes.isOk:
            for event in drainRes.get():
              case event.kind
              of mevPublishAccepted:
                if event.commandId == publishIdRes.get():
                  sawAccepted = true
                  acceptedMid = event.mid
              of mevPublishCompleted:
                if event.commandId == publishIdRes.get():
                  sawCompleted = true
                  if acceptedMid != 0:
                    check event.mid == acceptedMid
              of mevMessageReceived:
                if event.message.topic == topic:
                  sawMessage = true
                  check event.message.payloadString() == "hello-highlevel"
              of mevError:
                checkpoint event.summary()
              else:
                discard

          if sawAccepted and sawCompleted and sawMessage:
            break
          await sleepAsync(10)

        check sawAccepted
        check sawCompleted
        check sawMessage

        let disconnectIdRes = client.disconnect()
        check disconnectIdRes.isOk
        if disconnectIdRes.isOk:
          let disconnectEvent = await waitForEvent(client, 200, mevDisconnected, disconnectIdRes.get())
          check disconnectEvent.isSome

        let stopIdRes = client.requestStop()
        check stopIdRes.isOk
        if stopIdRes.isOk:
          let stopEvent = await waitForEvent(client, 200, mevStopped, stopIdRes.get())
          check stopEvent.isSome

        check client.joinMqttClient().isOk
        return sawAccepted and sawCompleted and sawMessage

      check waitFor scenario()



when getEnv("MOSQUITTO_NIM_TEST_BROKER") == "1":
  suite "mosquitto_nim highlevel subscribe handler broker test":
    test "subscribe with handler registers callback and broker subscription":
      proc waitForEvent(client: MqttClient; maxRounds: int;
                        wantedKind: MqttEventKind; commandId = -1): Future[Option[MqttEvent]] {.async.} =
        for _ in 0 ..< maxRounds:
          let drainRes = client.drainEvents()
          check drainRes.isOk
          if drainRes.isOk:
            for event in drainRes.get():
              if event.kind == wantedKind:
                if commandId >= 0 and event.commandId != commandId:
                  continue
                return some(event)
          await sleepAsync(10)

        return none(MqttEvent)

      proc scenario(): Future[bool] {.async.} =
        let host = getEnv("MOSQUITTO_NIM_TEST_HOST", "127.0.0.1")
        let port = parseInt(getEnv("MOSQUITTO_NIM_TEST_PORT", "1883"))
        let topic = "mosquitto_nim/step12/handler/" & $getTime().toUnix() & "_" & $getCurrentProcessId()

        let clientRes = startMqttClient("mosquitto_nim_step12_handler", loopTimeoutMs = 10, pollMs = 1)
        check clientRes.isOk
        if clientRes.isErr:
          return false

        let client = clientRes.get()
        var handlerPayload = ""
        var handlerTopic = ""

        let connectIdRes = client.connect(host, port = port, keepalive = 30)
        check connectIdRes.isOk
        if connectIdRes.isErr:
          discard client.requestStop()
          discard client.joinMqttClient()
          return false
        let connectEvent = await waitForEvent(client, 300, mevConnected, connectIdRes.get())
        check connectEvent.isSome

        let handler: MqttMessageHandler = proc(message: MqttMessage) =
          handlerTopic = message.topic
          handlerPayload = message.payloadString()

        let subRes = client.subscribe(topic, qos1, handler)
        check subRes.isOk
        if subRes.isErr:
          discard client.requestStop()
          discard client.joinMqttClient()
          return false
        check subRes.get().handlerId > 0
        check subRes.get().commandId > 0
        check client.messageHandlerCount() == 1

        let subscribeEvent = await waitForEvent(client, 300, mevSubscribed, subRes.get().commandId)
        check subscribeEvent.isSome

        let publishIdRes = client.publish(topic, "hello-handler", qos1, retain = false)
        check publishIdRes.isOk
        if publishIdRes.isErr:
          discard client.requestStop()
          discard client.joinMqttClient()
          return false

        var sawMessage = false
        for _ in 0 ..< 400:
          let drainRes = client.drainEvents()
          check drainRes.isOk
          if drainRes.isOk:
            for event in drainRes.get():
              if event.kind == mevMessageReceived and event.message.topic == topic:
                sawMessage = true
              let dispatchRes = await client.dispatchEvent(event)
              check dispatchRes.isOk
          if sawMessage and handlerPayload == "hello-handler":
            break
          await sleepAsync(10)

        check sawMessage
        check handlerTopic == topic
        check handlerPayload == "hello-handler"

        let unsubRes = client.unsubscribe(subRes.get())
        check unsubRes.isOk
        if unsubRes.isOk:
          let unsubEvent = await waitForEvent(client, 300, mevUnsubscribed, unsubRes.get())
          check unsubEvent.isSome
        check client.messageHandlerCount() == 0

        let disconnectIdRes = client.disconnect()
        check disconnectIdRes.isOk
        if disconnectIdRes.isOk:
          discard await waitForEvent(client, 200, mevDisconnected, disconnectIdRes.get())

        let stopIdRes = client.requestStop()
        check stopIdRes.isOk
        if stopIdRes.isOk:
          discard await waitForEvent(client, 200, mevStopped, stopIdRes.get())

        check client.joinMqttClient().isOk
        return sawMessage and handlerTopic == topic and handlerPayload == "hello-handler"

      check waitFor scenario()


suite "mosquitto_nim nmqtt compatibility facade":
  test "nmqtt context stores basic configuration":
    let ctx = newMqttCtx("mosquitto_nim_step13_config")
    check not ctx.isConnected()
    check ctx.msgQueue() == 0
    ctx.set_host("127.0.0.1", 1883)
    ctx.set_ping_interval(30)
    ctx.set_auth("compat-user", "compat-pass")
    ctx.set_will("mosquitto_nim/compat/will", "offline", qos = 1, retain = true)
    check ctx.lastError().isNone

suite "mosquitto_nim MQTT v5 property helpers":
  test "MQTT v5 properties are Nim-owned values":
    let userProp = userProperty("trace-id", "step19")
    check userProp.kind == mpUserProperty
    check userProp.name == "trace-id"
    check userProp.value == "step19"

    let responseProp = responseTopic("mosquitto_nim/reply")
    check responseProp.kind == mpResponseTopic
    check responseProp.value == "mosquitto_nim/reply"

    let correlationProp = correlationData("correlation-1")
    check correlationProp.kind == mpCorrelationData
    check correlationProp.propertyDataString() == "correlation-1"

    let expiryProp = messageExpiryInterval(60'u32)
    check expiryProp.kind == mpMessageExpiryInterval
    check expiryProp.intValue == 60'u32

    let contentTypeProp = contentType("application/json")
    check contentTypeProp.kind == mpContentType
    check contentTypeProp.value == "application/json"

    let payloadFormatProp = payloadFormatIndicatorUtf8()
    check payloadFormatProp.kind == mpPayloadFormatIndicator
    check payloadFormatProp.intValue == 1'u32
    check payloadFormatIndicatorUnspecified().intValue == 0'u32
    check toMqttPayloadFormatIndicator(1'u32).get() == mpfiUtf8
    check toMqttPayloadFormatIndicator(2'u32).isErr

    let cmd = publishV5Command(
      "mosquitto_nim/step19/property",
      "hello",
      qos = qos1,
      properties = @[
        userProp,
        responseProp,
        correlationProp,
        expiryProp,
        contentTypeProp,
        payloadFormatProp
      ]
    )
    check cmd.properties.len == 6
    check cmd.properties[0].name == "trace-id"
    check cmd.properties[1].value == "mosquitto_nim/reply"
    check cmd.properties[2].propertyDataString() == "correlation-1"
    check cmd.properties[3].intValue == 60'u32
    check cmd.properties[4].value == "application/json"
    check cmd.properties[5].intValue == 1'u32

  test "typed MQTT v5 publish properties convert to generic properties":
    let typedRes = mqttPublishProperties(
      userProperty("trace-id", "step29"),
      responseTopic("mosquitto_nim/step29/reply"),
      correlationData("corr-step29"),
      messageExpiryInterval(90'u32),
      contentType("application/json"),
      payloadFormatIndicatorUtf8()
    )
    check typedRes.isOk
    if typedRes.isOk:
      let typed = typedRes.get()
      check typed.userProperties.len == 1
      check typed.userProperties[0][0] == "trace-id"
      check typed.userProperties[0][1] == "step29"
      check typed.responseTopic.get() == "mosquitto_nim/step29/reply"
      check typed.correlationData.get().len == "corr-step29".len
      check typed.messageExpiryInterval.get() == 90'u32
      check typed.contentType.get() == "application/json"
      check typed.payloadFormatIndicator.get() == mpfiUtf8

      let generic = typed.toMqttProperties()
      check generic.len == 6
      check generic[0].kind == mpUserProperty
      check generic[1].kind == mpResponseTopic
      check generic[2].kind == mpCorrelationData
      check generic[3].kind == mpMessageExpiryInterval
      check generic[4].kind == mpContentType
      check generic[5].kind == mpPayloadFormatIndicator

      let cmd = publishV5Command(
        "mosquitto_nim/step29/typed",
        "hello-typed",
        qos = qos1,
        properties = typed
      )
      check cmd.properties.len == 6
      check cmd.properties[1].value == "mosquitto_nim/step29/reply"

  test "typed MQTT v5 publish properties reject duplicate single-instance properties":
    check mqttPublishProperties(responseTopic("a"), responseTopic("b")).isErr
    check mqttPublishProperties(correlationData("a"), correlationData("b")).isErr
    check mqttPublishProperties(messageExpiryInterval(1'u32), messageExpiryInterval(2'u32)).isErr
    check mqttPublishProperties(contentType("text/plain"), contentType("application/json")).isErr
    check mqttPublishProperties(payloadFormatIndicatorUtf8(), payloadFormatIndicatorUnspecified()).isErr
    check mqttPublishProperties(userProperty("a", "1"), userProperty("b", "2")).isOk

  test "typed MQTT v5 publish properties can be built mutably":
    var typed = noPublishProperties()
    typed.addUserProperty("trace-id", "step29-mutable")
    typed.setResponseTopic("mosquitto_nim/step29/mutable/reply")
    typed.setCorrelationData("corr-step29-mutable")
    typed.setMessageExpiryInterval(120'u32)
    typed.setContentType("text/plain")
    typed.setPayloadFormatIndicator(mpfiUtf8)

    let generic = typed.toMqttProperties()
    check generic.len == 6
    check generic[0].name == "trace-id"
    check generic[1].value == "mosquitto_nim/step29/mutable/reply"
    check generic[2].propertyDataString() == "corr-step29-mutable"
    check generic[3].intValue == 120'u32
    check generic[4].value == "text/plain"
    check generic[5].intValue == 1'u32

  test "supported MQTT v5 publish properties build into libmosquitto properties":
    let props = @[
      userProperty("trace-id", "step28"),
      responseTopic("mosquitto_nim/reply"),
      correlationData("correlation-step28"),
      messageExpiryInterval(30'u32),
      contentType("application/json"),
      payloadFormatIndicatorUtf8()
    ]
    let rawRes = buildMosquittoProperties(props)
    check rawRes.isOk
    if rawRes.isOk:
      var raw = rawRes.get()
      check raw != nil
      let copiedRes = copyProperties(raw)
      check copiedRes.isOk
      if copiedRes.isOk:
        let copied = copiedRes.get()
        check copied.len == 6
        check copied[0].kind == mpPayloadFormatIndicator
        check copied[0].intValue == 1'u32
        check copied[1].kind == mpMessageExpiryInterval
        check copied[1].intValue == 30'u32
        check copied[2].kind == mpContentType
        check copied[2].value == "application/json"
        check copied[3].kind == mpUserProperty
        check copied[3].name == "trace-id"
        check copied[4].kind == mpResponseTopic
        check copied[5].kind == mpCorrelationData
        check copied[5].propertyDataString() == "correlation-step28"
      freeMosquittoProperties(raw)

  test "invalid MQTT v5 properties are rejected by lowlevel property builder":
    check buildMosquittoProperties(@[userProperty("", "bad")]).isErr
    check buildMosquittoProperties(@[responseTopic("")]).isErr
    check buildMosquittoProperties(@[responseTopic("mosquitto_nim/+")]).isErr
    check buildMosquittoProperties(@[payloadFormatIndicator(2'u8)]).isErr


when getEnv("MOSQUITTO_NIM_TEST_BROKER") == "1":
  suite "mosquitto_nim nmqtt compatibility broker test":
    test "nmqtt-style start subscribe publish callback disconnect works":
      proc scenario(): Future[bool] {.async.} =
        let host = getEnv("MOSQUITTO_NIM_TEST_HOST", "127.0.0.1")
        let port = parseInt(getEnv("MOSQUITTO_NIM_TEST_PORT", "1883"))
        let topic = "mosquitto_nim/step13/nmqtt_compat/" & $getTime().toUnix() & "_" & $getCurrentProcessId()

        let ctx = newMqttCtx("mosquitto_nim_step13_nmqtt")
        ctx.set_host(host, port)
        ctx.set_ping_interval(30)
        ctx.set_will("mosquitto_nim/step15/will", "offline", qos = 1, retain = false)

        var receivedTopic = ""
        var receivedPayload = ""
        proc onData(topic: string; message: string) =
          receivedTopic = topic
          receivedPayload = message

        await ctx.start()
        check ctx.isConnected()

        await ctx.subscribe(topic, 1, onData)
        await sleepAsync(100)

        await ctx.publish(topic, "hello-nmqtt-compat", 1, retain = false)

        for _ in 0 ..< 400:
          if receivedPayload == "hello-nmqtt-compat":
            break
          await sleepAsync(10)

        check receivedTopic == topic
        check receivedPayload == "hello-nmqtt-compat"

        for _ in 0 ..< 400:
          if ctx.msgQueue() == 0:
            break
          await sleepAsync(10)
        check ctx.msgQueue() == 0

        await ctx.unsubscribe(topic)
        await sleepAsync(50)
        await ctx.disconnect()
        check not ctx.isConnected()
        return receivedTopic == topic and receivedPayload == "hello-nmqtt-compat"

      check waitFor scenario()

when getEnv("MOSQUITTO_NIM_TEST_BROKER") == "1":
  suite "mosquitto_nim MQTT v5 broker smoke test":
    test "lowlevel client can connect and exchange messages with MQTT v5":
      let host = getEnv("MOSQUITTO_NIM_TEST_HOST", "127.0.0.1")
      let port = parseInt(getEnv("MOSQUITTO_NIM_TEST_PORT", "1883"))
      let topic = "mosquitto_nim/step18/v5/lowlevel/" & $getTime().toUnix() & "_" & $getCurrentProcessId()

      check initLibrary().isOk

      let clientRes = newLowLevelClient("mosquitto_nim_step18_v5_lowlevel")
      check clientRes.isOk
      let client = clientRes.get()

      var received: seq[MqttMessage] = @[]
      let sink: MessageSink = proc(message: MqttMessage) =
        received.add(message)

      check setProtocolVersion(client, mpv5).isOk
      check setMessageSink(client, sink).isOk
      check connectLowLevelClient(client, host, port).isOk

      for _ in 0 ..< 10:
        check loopLowLevelClient(client, timeoutMs = 20).isOk

      let subRes = subscribeLowLevelClient(client, topic, qos1)
      check subRes.isOk

      for _ in 0 ..< 10:
        check loopLowLevelClient(client, timeoutMs = 20).isOk

      let pubRes = publishLowLevelClient(client, topic, "hello-v5-lowlevel", qos1)
      check pubRes.isOk

      for _ in 0 ..< 80:
        check loopLowLevelClient(client, timeoutMs = 20).isOk
        if received.len > 0:
          break

      check lastCallbackError(client).isNone
      check received.len == 1
      if received.len == 1:
        check received[0].topic == topic
        check received[0].payloadString() == "hello-v5-lowlevel"
        check received[0].qos == qos1

      discard disconnectLowLevelClient(client)
      discard closeLowLevelClient(client)
      check cleanupLibrary().isOk

    test "nmqtt compatibility facade can use MQTT v5 explicitly":
      proc scenario(): Future[bool] {.async.} =
        let host = getEnv("MOSQUITTO_NIM_TEST_HOST", "127.0.0.1")
        let port = parseInt(getEnv("MOSQUITTO_NIM_TEST_PORT", "1883"))
        let topic = "mosquitto_nim/step18/v5/nmqtt_compat/" & $getTime().toUnix() & "_" & $getCurrentProcessId()

        let ctx = newMqttCtx("mosquitto_nim_step18_v5_nmqtt")
        ctx.set_host(host, port)
        ctx.setProtocolVersion(mpv5)
        ctx.set_ping_interval(30)

        var receivedTopic = ""
        var receivedPayload = ""
        proc onData(topic: string; message: string) =
          receivedTopic = topic
          receivedPayload = message

        await ctx.start()
        check ctx.isConnected()

        await ctx.subscribe(topic, 1, onData)
        await sleepAsync(100)

        await ctx.publish(topic, "hello-v5-nmqtt-compat", 1, retain = false)

        for _ in 0 ..< 400:
          if receivedPayload == "hello-v5-nmqtt-compat":
            break
          await sleepAsync(10)

        check receivedTopic == topic
        check receivedPayload == "hello-v5-nmqtt-compat"

        for _ in 0 ..< 400:
          if ctx.msgQueue() == 0:
            break
          await sleepAsync(10)
        check ctx.msgQueue() == 0

        await ctx.unsubscribe(topic)
        await sleepAsync(50)
        await ctx.disconnect()
        check not ctx.isConnected()
        return receivedTopic == topic and receivedPayload == "hello-v5-nmqtt-compat"

      check waitFor scenario()



when getEnv("MOSQUITTO_NIM_TEST_BROKER") == "1":
  suite "mosquitto_nim MQTT v5 user property broker test":
    test "lowlevel v5 publish can send and receive User Property":
      let host = getEnv("MOSQUITTO_NIM_TEST_HOST", "127.0.0.1")
      let port = parseInt(getEnv("MOSQUITTO_NIM_TEST_PORT", "1883"))
      let unique = $getTime().toUnix() & "_" & $getCurrentProcessId()
      let topic = "mosquitto_nim/step19/v5/user_property/lowlevel/" & unique

      check initLibrary().isOk

      let clientRes = newLowLevelClient("mosquitto_nim_step19_v5_property_lowlevel")
      check clientRes.isOk
      let client = clientRes.get()

      var received: seq[MqttMessage] = @[]
      let sink: MessageSink = proc(message: MqttMessage) =
        received.add(message)

      check setProtocolVersion(client, mpv5).isOk
      check setMessageSink(client, sink).isOk
      check connectLowLevelClient(client, host, port).isOk

      for _ in 0 ..< 10:
        check loopLowLevelClient(client, timeoutMs = 20).isOk

      let subRes = subscribeLowLevelClient(client, topic, qos1)
      check subRes.isOk

      for _ in 0 ..< 10:
        check loopLowLevelClient(client, timeoutMs = 20).isOk

      let responseTopicName = "mosquitto_nim/step20/v5/reply/" & unique
      let props = @[
        userProperty("trace-id", "step19-lowlevel"),
        responseTopic(responseTopicName),
        correlationData("corr-step20-lowlevel"),
        messageExpiryInterval(45'u32),
        contentType("text/plain"),
        payloadFormatIndicatorUtf8()
      ]
      let pubRes = publishLowLevelClientV5(
        client,
        topic,
        "hello-v5-user-property-lowlevel",
        qos1,
        retain = false,
        properties = props
      )
      check pubRes.isOk

      for _ in 0 ..< 100:
        check loopLowLevelClient(client, timeoutMs = 20).isOk
        if received.len > 0:
          break

      check lastCallbackError(client).isNone
      check received.len == 1
      if received.len == 1:
        check received[0].topic == topic
        check received[0].payloadString() == "hello-v5-user-property-lowlevel"
        var sawUserProperty = false
        var sawResponseTopic = false
        var sawCorrelationData = false
        var sawMessageExpiry = false
        var sawContentType = false
        var sawPayloadFormat = false
        for property in received[0].properties:
          case property.kind
          of mpUserProperty:
            if property.name == "trace-id" and property.value == "step19-lowlevel":
              sawUserProperty = true
          of mpResponseTopic:
            if property.value == responseTopicName:
              sawResponseTopic = true
          of mpCorrelationData:
            if property.propertyDataString() == "corr-step20-lowlevel":
              sawCorrelationData = true
          of mpMessageExpiryInterval:
            if property.intValue == 45'u32:
              sawMessageExpiry = true
          of mpContentType:
            if property.value == "text/plain":
              sawContentType = true
          of mpPayloadFormatIndicator:
            if property.intValue == 1'u32:
              sawPayloadFormat = true
          else:
            discard
        check sawUserProperty
        check sawResponseTopic
        check sawCorrelationData
        check sawMessageExpiry
        check sawContentType
        check sawPayloadFormat

      discard disconnectLowLevelClient(client)
      discard closeLowLevelClient(client)
      check cleanupLibrary().isOk

    test "nmqtt compatibility extension can publish MQTT v5 User Property":
      proc scenario(): Future[bool] {.async.} =
        let host = getEnv("MOSQUITTO_NIM_TEST_HOST", "127.0.0.1")
        let port = parseInt(getEnv("MOSQUITTO_NIM_TEST_PORT", "1883"))
        let topic = "mosquitto_nim/step19/v5/user_property/nmqtt_compat/" & $getTime().toUnix() & "_" & $getCurrentProcessId()

        let ctx = newMqttCtx("mosquitto_nim_step19_v5_property_nmqtt")
        ctx.set_host(host, port)
        ctx.setProtocolVersion(mpv5)

        var receivedTopic = ""
        var receivedPayload = ""
        proc onData(topic: string; message: string) =
          receivedTopic = topic
          receivedPayload = message

        await ctx.start()
        check ctx.isConnected()

        await ctx.subscribe(topic, 1, onData)
        await sleepAsync(100)

        await ctx.publishV5(
          topic,
          "hello-v5-user-property-nmqtt-compat",
          1,
          retain = false,
          properties = @[
            userProperty("trace-id", "step19-nmqtt"),
            responseTopic("mosquitto_nim/step20/v5/reply/nmqtt"),
            correlationData("corr-step20-nmqtt"),
            messageExpiryInterval(30'u32),
            contentType("text/plain"),
            payloadFormatIndicatorUtf8()
          ]
        )

        for _ in 0 ..< 400:
          if receivedPayload == "hello-v5-user-property-nmqtt-compat":
            break
          await sleepAsync(10)

        check receivedTopic == topic
        check receivedPayload == "hello-v5-user-property-nmqtt-compat"

        for _ in 0 ..< 400:
          if ctx.msgQueue() == 0:
            break
          await sleepAsync(10)
        check ctx.msgQueue() == 0

        await ctx.unsubscribe(topic)
        await sleepAsync(50)
        await ctx.disconnect()
        check not ctx.isConnected()
        return receivedTopic == topic and receivedPayload == "hello-v5-user-property-nmqtt-compat"

      check waitFor scenario()


suite "mosquitto_nim connection state tracking":
  test "connection state helpers and state events are available":
    check mcsConnected.isConnected()
    check not mcsConnecting.isConnected()
    check mcsStopped.isTerminal()
    check mcsError.isTerminal()

    let ev = stateChangedEvent(mcsConnecting, commandId = 501, detail = "unit-test")
    check ev.kind == mevStateChanged
    check ev.state == mcsConnecting
    check ev.commandId == 501
    check ev.summary().contains("mcsConnecting")

  test "highlevel client tracks stop state through async events":
    proc scenario(): Future[bool] {.async.} =
      let clientRes = startMqttClient("mosquitto_nim_step22_state_stop", loopTimeoutMs = 10, pollMs = 1)
      check clientRes.isOk
      if clientRes.isErr:
        return false

      let client = clientRes.get()
      check client.currentState() == mcsDisconnected
      check not client.isConnected()

      let stopIdRes = client.requestStop()
      check stopIdRes.isOk
      if stopIdRes.isErr:
        discard client.joinMqttClient()
        return false
      check client.currentState() == mcsStopping

      var sawStoppedState = false
      var sawStoppedEvent = false
      for _ in 0 ..< 200:
        let eventRes = await client.nextEvent()
        check eventRes.isOk
        if eventRes.isErr:
          break

        let event = eventRes.get()
        if event.kind == mevStateChanged and event.state == mcsStopped:
          sawStoppedState = true
        if event.kind == mevStopped and event.commandId == stopIdRes.get():
          sawStoppedEvent = true
        if sawStoppedState and sawStoppedEvent:
          break

      check sawStoppedState
      check sawStoppedEvent
      check client.currentState() == mcsStopped
      check client.joinMqttClient().isOk
      check client.currentState() == mcsStopped
      return sawStoppedState and sawStoppedEvent

    check waitFor scenario()


when getEnv("MOSQUITTO_NIM_TEST_BROKER") == "1":
  suite "mosquitto_nim connection state broker test":
    test "highlevel client tracks connect and disconnect states":
      proc waitForEvent(client: MqttClient; wantedKind: MqttEventKind;
                        commandId: int; maxRounds = 300): Future[Option[MqttEvent]] {.async.} =
        for _ in 0 ..< maxRounds:
          let eventRes = await client.nextEvent()
          check eventRes.isOk
          if eventRes.isErr:
            break

          let event = eventRes.get()
          if event.kind == wantedKind and event.commandId == commandId:
            return some(event)

        return none(MqttEvent)

      proc scenario(): Future[bool] {.async.} =
        let host = getEnv("MOSQUITTO_NIM_TEST_HOST", "127.0.0.1")
        let port = parseInt(getEnv("MOSQUITTO_NIM_TEST_PORT", "1883"))

        let clientRes = startMqttClient("mosquitto_nim_step22_state_broker", loopTimeoutMs = 10, pollMs = 1)
        check clientRes.isOk
        if clientRes.isErr:
          return false

        let client = clientRes.get()
        check client.currentState() == mcsDisconnected

        let connectIdRes = client.connect(host, port = port, keepalive = 30)
        check connectIdRes.isOk
        if connectIdRes.isErr:
          discard client.requestStop()
          discard client.joinMqttClient()
          return false
        check client.currentState() == mcsConnecting

        let connectedEvent = await waitForEvent(client, mevConnected, connectIdRes.get())
        check connectedEvent.isSome
        check client.currentState() == mcsConnected
        check client.isConnected()

        let disconnectIdRes = client.disconnect()
        check disconnectIdRes.isOk
        if disconnectIdRes.isErr:
          discard client.requestStop()
          discard client.joinMqttClient()
          return false
        check client.currentState() == mcsDisconnecting

        let disconnectedEvent = await waitForEvent(client, mevDisconnected, disconnectIdRes.get())
        check disconnectedEvent.isSome
        check client.currentState() == mcsDisconnected
        check not client.isConnected()

        let stopIdRes = client.requestStop()
        check stopIdRes.isOk
        let stoppedEvent = await waitForEvent(client, mevStopped, stopIdRes.get())
        check stoppedEvent.isSome
        check client.currentState() == mcsStopped
        check client.joinMqttClient().isOk
        return connectedEvent.isSome and disconnectedEvent.isSome and stoppedEvent.isSome

      check waitFor scenario()

suite "mosquitto_nim reconnect policy API":
  test "reconnect policy helpers validate enabled settings":
    let disabled = noReconnect()
    check not disabled.enabled
    check disabled.validateReconnectPolicy().isOk
    check ($disabled).contains("enabled=false")

    let enabled = mqttReconnectPolicy(initialDelayMs = 100, maxDelayMs = 5000, multiplier = 1.5)
    check enabled.enabled
    check enabled.initialDelayMs == 100
    check enabled.maxDelayMs == 5000
    check enabled.multiplier == 1.5
    check enabled.validateReconnectPolicy().isOk
    check ($enabled).contains("initialDelayMs=100")

    check validateReconnectPolicy(MqttReconnectPolicy(
      enabled: true,
      initialDelayMs: -1,
      maxDelayMs: 1000,
      multiplier: 2.0
    )).isErr
    check validateReconnectPolicy(MqttReconnectPolicy(
      enabled: true,
      initialDelayMs: 2000,
      maxDelayMs: 1000,
      multiplier: 2.0
    )).isErr
    check validateReconnectPolicy(MqttReconnectPolicy(
      enabled: true,
      initialDelayMs: 1000,
      maxDelayMs: 1000,
      multiplier: 0.5
    )).isErr

  test "reconnect backoff delay helper is capped by policy":
    let policy = mqttReconnectPolicy(initialDelayMs = 100, maxDelayMs = 1000, multiplier = 2.0)
    check policy.reconnectDelayMs(0) == 0
    check policy.reconnectDelayMs(1) == 100
    check policy.reconnectDelayMs(2) == 200
    check policy.reconnectDelayMs(3) == 400
    check policy.reconnectDelayMs(4) == 800
    check policy.reconnectDelayMs(5) == 1000
    check noReconnect().reconnectDelayMs(1) == 0

  test "reconnect events carry attempt and delay metadata":
    let scheduled = reconnectScheduledEvent(250, 3, commandId = 42, detail = "loop error")
    check scheduled.kind == mevReconnectScheduled
    check scheduled.state == mcsReconnecting
    check scheduled.commandId == 42
    check scheduled.reconnectDelayMs == 250
    check scheduled.reconnectAttempt == 3
    check scheduled.summary().contains("delayMs=250")

    let attempt = reconnectAttemptEvent(4, commandId = 42, detail = "retry")
    check attempt.kind == mevReconnectAttempt
    check attempt.state == mcsReconnecting
    check attempt.commandId == 42
    check attempt.reconnectAttempt == 4
    check attempt.summary().contains("attempt=4")

  test "highlevel client stores reconnect policy for future connect commands":
    let clientRes = startMqttClient("mosquitto_nim_step24_reconnect_policy", pollMs = 1)
    check clientRes.isOk

    if clientRes.isOk:
      let client = clientRes.get()
      check not client.reconnectPolicy().enabled

      let policy = mqttReconnectPolicy(initialDelayMs = 250, maxDelayMs = 8000, multiplier = 2.5)
      check client.setReconnectPolicy(policy).isOk
      check client.reconnectPolicy().enabled
      check client.reconnectPolicy().initialDelayMs == 250
      check client.reconnectPolicy().maxDelayMs == 8000
      check client.reconnectPolicy().multiplier == 2.5

      check client.setReconnectPolicy(MqttReconnectPolicy(
        enabled: true,
        initialDelayMs: 1000,
        maxDelayMs: 500,
        multiplier: 2.0
      )).isErr
      check client.reconnectPolicy().maxDelayMs == 8000

      check client.disableReconnect().isOk
      check not client.reconnectPolicy().enabled

      let stopRes = client.requestStop()
      check stopRes.isOk
      if stopRes.isOk:
        var sawStopped = false
        for _ in 0 ..< 100:
          let drainRes = client.drainEvents()
          check drainRes.isOk
          if drainRes.isOk:
            for event in drainRes.get():
              if event.kind == mevStopped and event.commandId == stopRes.get():
                sawStopped = true
                break
          if sawStopped:
            break
          sleep(10)
        check sawStopped

      check client.joinMqttClient().isOk

  test "nmqtt compatibility context defaults to reconnect and stores reconnect policy":
    let ctx = newMqttCtx("mosquitto_nim_step24_reconnect_nmqtt")
    check ctx.reconnectPolicy().enabled
    check ctx.reconnectPolicy().initialDelayMs == 1000
    check ctx.reconnectPolicy().maxDelayMs == 30000
    check ctx.reconnectPolicy().multiplier == 2.0
    check ctx.enableReconnect(initialDelayMs = 300, maxDelayMs = 9000, multiplier = 3.0).isOk
    check ctx.reconnectPolicy().enabled
    check ctx.reconnectPolicy().initialDelayMs == 300
    check ctx.reconnectPolicy().maxDelayMs == 9000
    check ctx.reconnectPolicy().multiplier == 3.0

    let invalidRes = ctx.setReconnectPolicy(MqttReconnectPolicy(
      enabled: true,
      initialDelayMs: 1000,
      maxDelayMs: 1000,
      multiplier: 0.75
    ))
    check invalidRes.isErr
    check ctx.lastError().isSome
    check ctx.reconnectPolicy().multiplier == 3.0

    check ctx.disableReconnect().isOk
    check not ctx.reconnectPolicy().enabled

suite "mosquitto_nim offline queue policy API":
  test "offline queue policy helpers validate enabled settings":
    let disabled = noOfflineQueue()
    check not disabled.enabled
    check disabled.qos0Policy == moqReject
    check disabled.validateOfflineQueuePolicy().isOk
    check ($disabled).contains("enabled=false")

    let defaultEnabled = mqttOfflineQueuePolicy()
    check defaultEnabled.enabled
    check defaultEnabled.maxMessages == 100
    check defaultEnabled.maxBytes == 1024 * 1024
    check defaultEnabled.qos0Policy == moqQueue

    let enabled = mqttOfflineQueuePolicy(maxMessages = 20, maxBytes = 4096, qos0Policy = moqDropOldest)
    check enabled.enabled
    check enabled.maxMessages == 20
    check enabled.maxBytes == 4096
    check enabled.qos0Policy == moqDropOldest
    check enabled.validateOfflineQueuePolicy().isOk
    check ($enabled).contains("maxMessages=20")

    check validateOfflineQueuePolicy(MqttOfflineQueuePolicy(
      enabled: true,
      maxMessages: 0,
      maxBytes: 4096,
      qos0Policy: moqReject
    )).isErr
    check validateOfflineQueuePolicy(MqttOfflineQueuePolicy(
      enabled: true,
      maxMessages: 10,
      maxBytes: 0,
      qos0Policy: moqQueue
    )).isErr

  test "queue snapshots combine pending and offline publish counts":
    let pending = pendingOperations(publishes = 1, subscribes = 2, unsubscribes = 0)
    let snapshot = queueSnapshot(pending = pending, offlineQueued = 3, offlineBytes = 128)
    check snapshot.pending.total == 3
    check snapshot.offlineQueued == 3
    check snapshot.offlineBytes == 128
    check snapshot.total == 6
    check not snapshot.isEmpty()

    let event = queueChangedEvent(snapshot, commandId = 77)
    check event.kind == mevQueueChanged
    check event.commandId == 77
    check event.queue.total == 6
    check event.pending.total == 3
    check event.summary().contains("offlineQueued=3")

  test "highlevel client stores offline queue policy for future connect commands":
    let clientRes = startMqttClient("mosquitto_nim_step26_offline_queue_policy", pollMs = 1)
    check clientRes.isOk

    if clientRes.isOk:
      let client = clientRes.get()
      check not client.offlineQueuePolicy().enabled

      let policy = mqttOfflineQueuePolicy(maxMessages = 12, maxBytes = 8192, qos0Policy = moqQueue)
      check client.setOfflineQueuePolicy(policy).isOk
      check client.offlineQueuePolicy().enabled
      check client.offlineQueuePolicy().maxMessages == 12
      check client.offlineQueuePolicy().maxBytes == 8192
      check client.offlineQueuePolicy().qos0Policy == moqQueue

      check client.setOfflineQueuePolicy(MqttOfflineQueuePolicy(
        enabled: true,
        maxMessages: -1,
        maxBytes: 8192,
        qos0Policy: moqReject
      )).isErr
      check client.offlineQueuePolicy().maxMessages == 12

      check client.disableOfflineQueue().isOk
      check not client.offlineQueuePolicy().enabled

      let stopRes = client.requestStop()
      check stopRes.isOk
      if stopRes.isOk:
        var sawStopped = false
        for _ in 0 ..< 100:
          let drainRes = client.drainEvents()
          check drainRes.isOk
          if drainRes.isOk:
            for event in drainRes.get():
              if event.kind == mevStopped and event.commandId == stopRes.get():
                sawStopped = true
                break
          if sawStopped:
            break
          sleep(10)
        check sawStopped

      check client.joinMqttClient().isOk

  test "nmqtt compatibility context defaults to offline queueing and stores offline queue policy":
    let ctx = newMqttCtx("mosquitto_nim_step26_offline_queue_nmqtt")
    check ctx.offlineQueuePolicy().enabled
    check ctx.offlineQueuePolicy().maxMessages == 100
    check ctx.offlineQueuePolicy().maxBytes == 1024 * 1024
    check ctx.offlineQueuePolicy().qos0Policy == moqQueue
    check ctx.enableOfflineQueue(maxMessages = 30, maxBytes = 16384, qos0Policy = moqDropNewest).isOk
    check ctx.offlineQueuePolicy().enabled
    check ctx.offlineQueuePolicy().maxMessages == 30
    check ctx.offlineQueuePolicy().maxBytes == 16384
    check ctx.offlineQueuePolicy().qos0Policy == moqDropNewest

    let invalidRes = ctx.setOfflineQueuePolicy(MqttOfflineQueuePolicy(
      enabled: true,
      maxMessages: 10,
      maxBytes: -1,
      qos0Policy: moqReject
    ))
    check invalidRes.isErr
    check ctx.lastError().isSome
    check ctx.offlineQueuePolicy().maxMessages == 30

    check ctx.disableOfflineQueue().isOk
    check not ctx.offlineQueuePolicy().enabled


when getEnv("MOSQUITTO_NIM_TEST_RECONNECT_FAILURE") == "1":
  suite "mosquitto_nim auto reconnect failure-path test":
    test "highlevel client schedules reconnect after a failed connect attempt":
      proc scenario(): Future[bool] {.async.} =
        let clientRes = startMqttClient(
          "mosquitto_nim_step25_reconnect_failure",
          eventQueueLen = 128,
          loopTimeoutMs = 10,
          pollMs = 1
        )
        check clientRes.isOk
        if clientRes.isErr:
          return false

        let client = clientRes.get()
        check client.enableReconnect(initialDelayMs = 20, maxDelayMs = 20, multiplier = 1.0).isOk

        let connectIdRes = client.connect("127.0.0.1", port = 1, keepalive = 1)
        check connectIdRes.isOk
        if connectIdRes.isErr:
          discard client.requestStop()
          discard client.joinMqttClient()
          return false

        var sawScheduled = false
        var sawAttempt = false
        for _ in 0 ..< 200:
          let drainRes = client.drainEvents()
          check drainRes.isOk
          if drainRes.isOk:
            for event in drainRes.get():
              case event.kind
              of mevReconnectScheduled:
                if event.commandId == connectIdRes.get():
                  sawScheduled = true
                  check event.reconnectAttempt >= 1
                  check event.reconnectDelayMs == 20
                  check client.currentState() == mcsReconnecting
              of mevReconnectAttempt:
                if event.commandId == connectIdRes.get():
                  sawAttempt = true
                  check event.reconnectAttempt >= 1
              of mevConnected:
                checkpoint "port 1 unexpectedly accepted an MQTT connection; reconnect failure-path test is not meaningful on this host"
              else:
                discard
          if sawScheduled and sawAttempt:
            break
          await sleepAsync(10)

        check sawScheduled

        let disconnectIdRes = client.disconnect()
        check disconnectIdRes.isOk
        let stopIdRes = client.requestStop()
        check stopIdRes.isOk

        var sawStopped = false
        for _ in 0 ..< 100:
          let drainRes = client.drainEvents()
          check drainRes.isOk
          if drainRes.isOk:
            for event in drainRes.get():
              if event.kind == mevStopped and event.commandId == stopIdRes.get():
                sawStopped = true
                break
          if sawStopped:
            break
          await sleepAsync(10)

        check sawStopped
        check client.joinMqttClient().isOk
        return sawScheduled and sawStopped

      check waitFor scenario()

suite "mosquitto_nim pending operation tracking":
  test "pending operation snapshots summarize in-flight operations":
    let pending = pendingOperations(publishes = 2, subscribes = 1, unsubscribes = 3)
    check pending.publishes == 2
    check pending.subscribes == 1
    check pending.unsubscribes == 3
    check pending.total == 6
    check not pending.isEmpty()

    let empty = emptyPendingOperations()
    check empty.isEmpty()
    check empty.total == 0

    let ev = pendingChangedEvent(pending, commandId = 42)
    check ev.kind == mevPendingChanged
    check ev.commandId == 42
    check ev.pending.total == 6
    check ev.summary().contains("total=6")

  test "highlevel client exposes an empty pending snapshot before broker operations":
    let clientRes = startMqttClient("mosquitto_nim_step23_pending_empty", pollMs = 1)
    check clientRes.isOk

    if clientRes.isOk:
      let client = clientRes.get()
      check client.pendingOperations().isEmpty()
      check client.pendingTotal() == 0
      check client.msgQueue() == 0

      let stopRes = client.requestStop()
      check stopRes.isOk
      if stopRes.isOk:
        var sawStopped = false
        for _ in 0 ..< 100:
          let drainRes = client.drainEvents()
          check drainRes.isOk
          if drainRes.isOk:
            for event in drainRes.get():
              if event.kind == mevStopped and event.commandId == stopRes.get():
                sawStopped = true
                break
          if sawStopped:
            break
          sleep(10)
        check sawStopped

      check client.joinMqttClient().isOk

when getEnv("MOSQUITTO_NIM_TEST_BROKER") == "1":
  suite "mosquitto_nim pending operation broker test":
    test "highlevel client receives pending publish snapshots":
      proc waitForEvent(client: MqttClient; maxRounds: int;
                        wantedKind: MqttEventKind; commandId = -1): Future[Option[MqttEvent]] {.async.} =
        for _ in 0 ..< maxRounds:
          let drainRes = client.drainEvents()
          check drainRes.isOk
          if drainRes.isOk:
            for event in drainRes.get():
              if event.kind == wantedKind:
                if commandId >= 0 and event.commandId != commandId:
                  continue
                return some(event)
          await sleepAsync(10)

        return none(MqttEvent)

      proc scenario(): Future[bool] {.async.} =
        let host = getEnv("MOSQUITTO_NIM_TEST_HOST", "127.0.0.1")
        let port = parseInt(getEnv("MOSQUITTO_NIM_TEST_PORT", "1883"))
        let topic = "mosquitto_nim/step23/pending/" & $getTime().toUnix() & "_" & $getCurrentProcessId()

        let clientRes = startMqttClient("mosquitto_nim_step23_pending", loopTimeoutMs = 10, pollMs = 1)
        check clientRes.isOk
        if clientRes.isErr:
          return false

        let client = clientRes.get()
        let connectIdRes = client.connect(host, port = port, keepalive = 30)
        check connectIdRes.isOk
        if connectIdRes.isErr:
          discard client.requestStop()
          discard client.joinMqttClient()
          return false

        let connectedEvent = await waitForEvent(client, 300, mevConnected, connectIdRes.get())
        check connectedEvent.isSome
        if connectedEvent.isNone:
          discard client.requestStop()
          discard client.joinMqttClient()
          return false

        let publishIdRes = client.publish(topic, "hello-pending", qos1, retain = false)
        check publishIdRes.isOk
        if publishIdRes.isErr:
          discard client.requestStop()
          discard client.joinMqttClient()
          return false

        var sawPendingNonZero = false
        var sawPendingZero = false
        var sawCompleted = false
        for _ in 0 ..< 400:
          let drainRes = client.drainEvents()
          check drainRes.isOk
          if drainRes.isOk:
            for event in drainRes.get():
              case event.kind
              of mevPendingChanged:
                if event.commandId == publishIdRes.get():
                  if event.pending.publishes > 0:
                    sawPendingNonZero = true
                  if event.pending.total == 0:
                    sawPendingZero = true
              of mevPublishCompleted:
                if event.commandId == publishIdRes.get():
                  sawCompleted = true
              of mevError:
                checkpoint event.summary()
              else:
                discard

          if sawPendingNonZero and sawPendingZero and sawCompleted:
            break
          await sleepAsync(10)

        check sawPendingNonZero
        check sawPendingZero
        check sawCompleted
        check client.pendingTotal() == 0

        let disconnectIdRes = client.disconnect()
        check disconnectIdRes.isOk
        if disconnectIdRes.isOk:
          discard await waitForEvent(client, 200, mevDisconnected, disconnectIdRes.get())

        let stopIdRes = client.requestStop()
        check stopIdRes.isOk
        if stopIdRes.isOk:
          discard await waitForEvent(client, 200, mevStopped, stopIdRes.get())

        check client.joinMqttClient().isOk
        return sawPendingNonZero and sawPendingZero and sawCompleted

      check waitFor scenario()
