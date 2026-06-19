# Destination: src/mosquitto_nim/worker/mosquitto_worker.nim

import std/[os, options, tables, typedthreads]

import results
import threadtools

import ../lowlevel/client
import ../lowlevel/errors
import ../lowlevel/library
import ../lowlevel/types
import ./types

# ------------------------------------------------------------------------------
# MQTT worker thread.
#
# This module owns a Nim-managed worker thread.  The worker creates the
# LowLevelClient inside that thread and keeps the libmosquitto handle there for
# its whole lifetime.  Callers communicate with the worker only through
# threadtools queues carrying pure Nim MqttCommand/MqttEvent values.
#
# PUBLISH timing is deliberately split:
#   - PublishAccepted: mosquitto_publish accepted the packet and returned a mid.
#   - PublishCompleted: libmosquitto on_publish callback fired later.
#
# nmqtt-compatible APIs can complete at queue/accepted time without waiting for
# PUBACK, while extended APIs can wait for PublishCompleted if they need it.
# ------------------------------------------------------------------------------
type
  MqttWorkerArgs = object
    clientId: string
    cleanSession: bool
    idleSleepMs: int
    loopTimeoutMs: int
    commandQueue: ThreadQueue[MqttCommand]
    eventQueue: ThreadQueue[MqttEvent]

  MqttWorker* = ref object
    commandQueue: ThreadQueue[MqttCommand]
    eventQueue: ThreadQueue[MqttEvent]
    thread: Thread[MqttWorkerArgs]
    started: bool
    joined: bool

# ------------------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------------------
proc queueError(context: string; code: ErrorCode): MqttError =
  case code
  of ErrorCode.Closed:
    result = makeError(meQueueClosed, context, $code, ord(code))
  of ErrorCode.Full:
    result = makeError(meQueueOverflow, context, $code, ord(code))
  else:
    result = makeError(meInvalidState, context, $code, ord(code))

proc sendWorkerEvent(eventQueue: ThreadQueue[MqttEvent]; event: sink MqttEvent) {.raises: [].} =
  ## Best-effort event send for the worker thread.
  ##
  ## This is deliberately non-raising.  A closed/full event queue means the owner
  ## is already tearing down or misconfigured; there is no safe higher-level place
  ## to report that from this low-level worker.
  if eventQueue.isNil or eventQueue.isClosed:
    return

  var ev = event
  discard eventQueue.sendMove(ev)

proc sendWorkerError(eventQueue: ThreadQueue[MqttEvent]; error: MqttError;
                     commandId = 0) {.raises: [].} =
  sendWorkerEvent(eventQueue, errorEvent(error, commandId))

proc flushLoop(client: LowLevelClient; eventQueue: ThreadQueue[MqttEvent];
               loopActive: var bool; loopTimeoutMs: int; rounds: int) {.raises: [].} =
  ## Run a few manual-loop iterations and report failures as worker errors.
  if not loopActive:
    return

  for _ in 0 ..< rounds:
    let loopRes = loopLowLevelClient(client, timeoutMs = loopTimeoutMs)
    if loopRes.isErr:
      loopActive = false
      sendWorkerError(eventQueue, loopRes.error)
      return

proc handleCommand(command: sink MqttCommand; running: var bool;
                   loopActive: var bool; pendingConnectId: var int;
                   pendingDisconnectId: var int;
                   pendingPublishes: var Table[int, int];
                   pendingSubscribes: var Table[int, int];
                   pendingUnsubscribes: var Table[int, int];
                   client: LowLevelClient;
                   eventQueue: ThreadQueue[MqttEvent];
                   loopTimeoutMs: int) {.raises: [].} =
  ## Handle one worker command.
  ##
  ## libmosquitto calls are intentionally made only here, inside the worker
  ## thread that owns LowLevelClient.
  var cmd = command

  case cmd.kind
  of mckStop:
    if loopActive:
      discard disconnectLowLevelClient(client)
      flushLoop(client, eventQueue, loopActive, loopTimeoutMs, rounds = 2)
      loopActive = false
    running = false
    sendWorkerEvent(eventQueue, stoppedEvent(commandId = cmd.id))

  of mckConnect:
    if loopActive:
      sendWorkerError(
        eventQueue,
        invalidState("MQTT worker connect", "client is already connected or connecting"),
        cmd.id
      )
      return

    let authRes = setUsernamePassword(client, cmd.username, cmd.password)
    if authRes.isErr:
      pendingConnectId = 0
      sendWorkerError(eventQueue, authRes.error, cmd.id)
      return

    pendingConnectId = cmd.id
    let connectRes = connectLowLevelClient(client, cmd.host, cmd.port, cmd.keepalive)
    if connectRes.isErr:
      pendingConnectId = 0
      sendWorkerError(eventQueue, connectRes.error, cmd.id)
      return

    loopActive = true
    flushLoop(client, eventQueue, loopActive, loopTimeoutMs, rounds = 3)

  of mckDisconnect:
    if not loopActive:
      sendWorkerEvent(eventQueue, disconnectedEvent(commandId = cmd.id, detail = "already disconnected"))
      return

    pendingDisconnectId = cmd.id
    let disconnectRes = disconnectLowLevelClient(client)
    if disconnectRes.isErr:
      pendingDisconnectId = 0
      sendWorkerError(eventQueue, disconnectRes.error, cmd.id)
      loopActive = false
      return

    flushLoop(client, eventQueue, loopActive, loopTimeoutMs, rounds = 2)

  of mckPublish:
    if not loopActive:
      sendWorkerError(eventQueue, invalidState("MQTT worker publish", "client is not connected"), cmd.id)
      return

    let publishRes = publishLowLevelClient(client, cmd.topic, cmd.payload, cmd.qos, cmd.retain)
    if publishRes.isErr:
      sendWorkerError(eventQueue, publishRes.error, cmd.id)
      return

    let mid = publishRes.get()
    if mid != 0:
      pendingPublishes[mid] = cmd.id
    sendWorkerEvent(eventQueue, publishAcceptedEvent(mid, commandId = cmd.id))

  of mckSubscribe:
    if not loopActive:
      sendWorkerError(eventQueue, invalidState("MQTT worker subscribe", "client is not connected"), cmd.id)
      return

    let subscribeRes = subscribeLowLevelClient(client, cmd.topic, cmd.qos)
    if subscribeRes.isErr:
      sendWorkerError(eventQueue, subscribeRes.error, cmd.id)
      return

    let mid = subscribeRes.get()
    if mid != 0:
      pendingSubscribes[mid] = cmd.id

  of mckUnsubscribe:
    if not loopActive:
      sendWorkerError(eventQueue, invalidState("MQTT worker unsubscribe", "client is not connected"), cmd.id)
      return

    let unsubscribeRes = unsubscribeLowLevelClient(client, cmd.topic)
    if unsubscribeRes.isErr:
      sendWorkerError(eventQueue, unsubscribeRes.error, cmd.id)
      return

    let mid = unsubscribeRes.get()
    if mid != 0:
      pendingUnsubscribes[mid] = cmd.id

proc workerMain(args: MqttWorkerArgs) {.thread.} =
  var stopCommandId = 0

  let initRes = initLibrary()
  if initRes.isErr:
    sendWorkerError(args.eventQueue, initRes.error)
    sendWorkerEvent(args.eventQueue, stoppedEvent(commandId = stopCommandId))
    return

  let clientRes = newLowLevelClient(args.clientId, args.cleanSession)
  if clientRes.isErr:
    sendWorkerError(args.eventQueue, clientRes.error)
    discard cleanupLibrary()
    sendWorkerEvent(args.eventQueue, stoppedEvent(commandId = stopCommandId))
    return

  let client = clientRes.get()
  var loopActive = false
  var running = true
  var pendingConnectId = 0
  var pendingDisconnectId = 0
  var pendingPublishes = initTable[int, int]()
  var pendingSubscribes = initTable[int, int]()
  var pendingUnsubscribes = initTable[int, int]()

  let messageSink: MessageSink = proc(message: MqttMessage) =
    var msg = message
    sendWorkerEvent(args.eventQueue, messageReceivedEvent(move msg))

  let controlSink: ControlSink = proc(event: LowLevelControlEvent) =
    case event.kind
    of lleConnected:
      let commandId = pendingConnectId
      pendingConnectId = 0
      if event.reasonCode == 0:
        loopActive = true
        sendWorkerEvent(
          args.eventQueue,
          connectedEvent(commandId = commandId, reasonCode = event.reasonCode, flags = event.flags)
        )
      else:
        loopActive = false
        sendWorkerError(args.eventQueue, protocolReason("MQTT connect callback", event.reasonCode), commandId)

    of lleDisconnected:
      let commandId = pendingDisconnectId
      pendingDisconnectId = 0
      loopActive = false
      sendWorkerEvent(
        args.eventQueue,
        disconnectedEvent(commandId = commandId, reasonCode = event.reasonCode)
      )

    of llePublishCompleted:
      let commandId = pendingPublishes.getOrDefault(event.mid, 0)
      if event.mid in pendingPublishes:
        pendingPublishes.del(event.mid)
      sendWorkerEvent(
        args.eventQueue,
        publishCompletedEvent(event.mid, commandId = commandId, reasonCode = event.reasonCode)
      )

    of lleSubscribed:
      let commandId = pendingSubscribes.getOrDefault(event.mid, 0)
      if event.mid in pendingSubscribes:
        pendingSubscribes.del(event.mid)
      sendWorkerEvent(
        args.eventQueue,
        subscribedEvent(event.mid, commandId = commandId, grantedQos = event.grantedQos)
      )

    of lleUnsubscribed:
      let commandId = pendingUnsubscribes.getOrDefault(event.mid, 0)
      if event.mid in pendingUnsubscribes:
        pendingUnsubscribes.del(event.mid)
      sendWorkerEvent(args.eventQueue, unsubscribedEvent(event.mid, commandId = commandId))

  let sinkRes = setMessageSink(client, messageSink)
  if sinkRes.isErr:
    sendWorkerError(args.eventQueue, sinkRes.error)
    running = false

  let controlSinkRes = setControlSink(client, controlSink)
  if controlSinkRes.isErr:
    sendWorkerError(args.eventQueue, controlSinkRes.error)
    running = false

  while running:
    var command: MqttCommand
    let recvRes = args.commandQueue.tryReceive(command)
    if recvRes.isErr:
      sendWorkerError(args.eventQueue, queueError("MQTT worker command queue", recvRes.error))
      break

    if recvRes.get():
      stopCommandId = command.id
      handleCommand(
        move command,
        running,
        loopActive,
        pendingConnectId,
        pendingDisconnectId,
        pendingPublishes,
        pendingSubscribes,
        pendingUnsubscribes,
        client,
        args.eventQueue,
        args.loopTimeoutMs
      )
    else:
      if loopActive:
        let loopRes = loopLowLevelClient(client, timeoutMs = args.loopTimeoutMs)
        if loopRes.isErr:
          loopActive = false
          sendWorkerError(args.eventQueue, loopRes.error)
      else:
        sleep(args.idleSleepMs)

    let callbackError = lastCallbackError(client)
    if callbackError.isSome():
      sendWorkerError(args.eventQueue, callbackError.get())
      discard clearCallbackError(client)

  if loopActive:
    discard disconnectLowLevelClient(client)
    loopActive = false

  discard clearControlSink(client)
  discard clearMessageSink(client)
  discard closeLowLevelClient(client)
  discard cleanupLibrary()

# ------------------------------------------------------------------------------
# Public worker API
# ------------------------------------------------------------------------------
proc startMqttWorker*(clientId = ""; cleanSession = true;
                      commandQueueLen = 32; eventQueueLen = 32;
                      idleSleepMs = 1; loopTimeoutMs = 10): MqttResult[MqttWorker] =
  ## Start a Nim-managed MQTT worker thread.
  ##
  ## The worker owns the LowLevelClient/libmosquitto handle.  Callers communicate
  ## with the worker through command/event queues only.
  if commandQueueLen <= 0:
    return err(invalidArgument("start MQTT worker", "commandQueueLen must be positive"))
  if eventQueueLen <= 0:
    return err(invalidArgument("start MQTT worker", "eventQueueLen must be positive"))
  if idleSleepMs < 0:
    return err(invalidArgument("start MQTT worker", "idleSleepMs must not be negative"))
  if loopTimeoutMs < 0:
    return err(invalidArgument("start MQTT worker", "loopTimeoutMs must not be negative"))

  let commandQueueRes = newThreadQueue[MqttCommand](commandQueueLen)
  if commandQueueRes.isErr:
    return err(queueError("new MQTT worker command queue", commandQueueRes.error))

  let eventQueueRes = newThreadQueue[MqttEvent](eventQueueLen)
  if eventQueueRes.isErr:
    return err(queueError("new MQTT worker event queue", eventQueueRes.error))

  var worker = MqttWorker(
    commandQueue: commandQueueRes.get(),
    eventQueue: eventQueueRes.get(),
    started: false,
    joined: false
  )

  createThread(worker.thread, workerMain, MqttWorkerArgs(
    clientId: clientId,
    cleanSession: cleanSession,
    idleSleepMs: idleSleepMs,
    loopTimeoutMs: loopTimeoutMs,
    commandQueue: worker.commandQueue,
    eventQueue: worker.eventQueue
  ))

  worker.started = true
  result = ok(worker)

proc isStarted*(worker: MqttWorker): bool =
  result = not worker.isNil and worker.started and not worker.joined

proc sendCommand*(worker: MqttWorker; command: sink MqttCommand): MqttResult[MqttOk] =
  if worker.isNil or not worker.isStarted:
    return err(invalidState("send MQTT worker command", "worker is not started"))

  var cmd = command
  let sendRes = worker.commandQueue.sendMove(cmd)
  if sendRes.isErr:
    return err(queueError("send MQTT worker command", sendRes.error))

  result = ok(MqttOk())

proc requestStop*(worker: MqttWorker; id = 0): MqttResult[MqttOk] =
  var cmd = stopCommand(id)
  result = worker.sendCommand(move cmd)

proc tryReceiveEvent*(worker: MqttWorker; event: var MqttEvent): MqttResult[bool] =
  if worker.isNil:
    return err(invalidState("receive MQTT worker event", "worker is nil"))

  let recvRes = worker.eventQueue.tryReceive(event)
  if recvRes.isErr:
    return err(queueError("receive MQTT worker event", recvRes.error))

  result = ok(recvRes.get())


proc eventQueueForAsyncBridge*(worker: MqttWorker): MqttResult[ThreadQueue[MqttEvent]] =
  ## Return the worker event queue for highlevel asyncdispatch bridging.
  ##
  ## This is intentionally narrower than exposing the libmosquitto handle.  The
  ## queue is receive-side state used by the highlevel bridge; callers must not
  ## send arbitrary events into it.
  if worker.isNil:
    return err(invalidState("get MQTT worker event queue", "worker is nil"))
  if worker.eventQueue.isNil:
    return err(invalidState("get MQTT worker event queue", "event queue is nil"))

  result = ok(worker.eventQueue)

proc closeQueues(worker: MqttWorker) =
  if worker.isNil:
    return

  if not worker.commandQueue.isNil:
    worker.commandQueue.close()
  if not worker.eventQueue.isNil:
    worker.eventQueue.close()

proc joinMqttWorker*(worker: MqttWorker): MqttResult[MqttOk] =
  if worker.isNil:
    return err(invalidState("join MQTT worker", "worker is nil"))
  if not worker.started:
    return ok(MqttOk())
  if worker.joined:
    return ok(MqttOk())

  joinThread(worker.thread)
  worker.joined = true
  worker.started = false
  worker.closeQueues()
  result = ok(MqttOk())
