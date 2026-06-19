# Destination: src/mosquitto_nim/worker/mosquitto_worker.nim

import std/[os, options, strformat, typedthreads]

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
# This step implements manual-loop Connect/Disconnect/Publish/Subscribe/
# Unsubscribe command handling.  It still does not use libmosquitto's internal
# threaded interface.
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
               connected: var bool; loopTimeoutMs: int; rounds: int) {.raises: [].} =
  ## Run a few manual-loop iterations and report failures as worker errors.
  if not connected:
    return

  for _ in 0 ..< rounds:
    let loopRes = loopLowLevelClient(client, timeoutMs = loopTimeoutMs)
    if loopRes.isErr:
      connected = false
      sendWorkerError(eventQueue, loopRes.error)
      return

proc handleCommand(command: sink MqttCommand; running: var bool;
                   connected: var bool; client: LowLevelClient;
                   eventQueue: ThreadQueue[MqttEvent];
                   loopTimeoutMs: int) {.raises: [].} =
  ## Handle one worker command.
  ##
  ## libmosquitto calls are intentionally made only here, inside the worker
  ## thread that owns LowLevelClient.
  var cmd = command

  case cmd.kind
  of mckStop:
    if connected:
      discard disconnectLowLevelClient(client)
      flushLoop(client, eventQueue, connected, loopTimeoutMs, rounds = 2)
      connected = false
    running = false
    sendWorkerEvent(eventQueue, stoppedEvent(commandId = cmd.id))

  of mckConnect:
    if connected:
      sendWorkerError(
        eventQueue,
        invalidState("MQTT worker connect", "client is already connected"),
        cmd.id
      )
      return

    let connectRes = connectLowLevelClient(client, cmd.host, cmd.port, cmd.keepalive)
    if connectRes.isErr:
      sendWorkerError(eventQueue, connectRes.error, cmd.id)
      return

    connected = true
    flushLoop(client, eventQueue, connected, loopTimeoutMs, rounds = 3)
    if connected:
      sendWorkerEvent(eventQueue, connectedEvent(commandId = cmd.id))

  of mckDisconnect:
    if not connected:
      sendWorkerEvent(eventQueue, disconnectedEvent(commandId = cmd.id, detail = "already disconnected"))
      return

    let disconnectRes = disconnectLowLevelClient(client)
    if disconnectRes.isErr:
      sendWorkerError(eventQueue, disconnectRes.error, cmd.id)
      connected = false
      return

    flushLoop(client, eventQueue, connected, loopTimeoutMs, rounds = 2)
    connected = false
    sendWorkerEvent(eventQueue, disconnectedEvent(commandId = cmd.id))

  of mckPublish:
    if not connected:
      sendWorkerError(eventQueue, invalidState("MQTT worker publish", "client is not connected"), cmd.id)
      return

    let publishRes = publishLowLevelClient(client, cmd.topic, cmd.payload, cmd.qos, cmd.retain)
    if publishRes.isErr:
      sendWorkerError(eventQueue, publishRes.error, cmd.id)
      return

    sendWorkerEvent(eventQueue, publishedEvent(publishRes.get(), commandId = cmd.id))

  of mckSubscribe:
    if not connected:
      sendWorkerError(eventQueue, invalidState("MQTT worker subscribe", "client is not connected"), cmd.id)
      return

    let subscribeRes = subscribeLowLevelClient(client, cmd.topic, cmd.qos)
    if subscribeRes.isErr:
      sendWorkerError(eventQueue, subscribeRes.error, cmd.id)
      return

    sendWorkerEvent(eventQueue, subscribedEvent(subscribeRes.get(), commandId = cmd.id))

  of mckUnsubscribe:
    if not connected:
      sendWorkerError(eventQueue, invalidState("MQTT worker unsubscribe", "client is not connected"), cmd.id)
      return

    let unsubscribeRes = unsubscribeLowLevelClient(client, cmd.topic)
    if unsubscribeRes.isErr:
      sendWorkerError(eventQueue, unsubscribeRes.error, cmd.id)
      return

    sendWorkerEvent(eventQueue, unsubscribedEvent(unsubscribeRes.get(), commandId = cmd.id))

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
  var connected = false
  var running = true

  let sink: MessageSink = proc(message: MqttMessage) =
    var msg = message
    sendWorkerEvent(args.eventQueue, messageReceivedEvent(move msg))

  let sinkRes = setMessageSink(client, sink)
  if sinkRes.isErr:
    sendWorkerError(args.eventQueue, sinkRes.error)
    running = false

  while running:
    var command: MqttCommand
    let recvRes = args.commandQueue.tryReceive(command)
    if recvRes.isErr:
      sendWorkerError(args.eventQueue, queueError("MQTT worker command queue", recvRes.error))
      break

    if recvRes.get():
      stopCommandId = command.id
      handleCommand(move command, running, connected, client, args.eventQueue, args.loopTimeoutMs)
    else:
      if connected:
        let loopRes = loopLowLevelClient(client, timeoutMs = args.loopTimeoutMs)
        if loopRes.isErr:
          connected = false
          sendWorkerError(args.eventQueue, loopRes.error)
      else:
        sleep(args.idleSleepMs)

    let callbackError = lastCallbackError(client)
    if callbackError.isSome():
      sendWorkerError(args.eventQueue, callbackError.get())
      discard clearCallbackError(client)

  if connected:
    discard disconnectLowLevelClient(client)
    connected = false

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
  ## Send a command to the worker thread.
  if worker.isNil or worker.commandQueue.isNil:
    return err(invalidState("send MQTT worker command", "worker is not initialized"))
  if worker.joined:
    return err(invalidState("send MQTT worker command", "worker is already joined"))

  var cmd = command
  let sendRes = worker.commandQueue.sendMove(cmd)
  if sendRes.isErr:
    return err(queueError("send MQTT worker command", sendRes.error))

  result = ok(MqttOk())

proc requestStop*(worker: MqttWorker; id = 0): MqttResult[MqttOk] =
  ## Request the worker to stop.
  result = sendCommand(worker, stopCommand(id = id))

proc tryReceiveEvent*(worker: MqttWorker; event: var MqttEvent): MqttResult[bool] =
  ## Try to receive one event from the worker without blocking.
  if worker.isNil or worker.eventQueue.isNil:
    return err(invalidState("receive MQTT worker event", "worker is not initialized"))

  let recvRes = worker.eventQueue.tryReceive(event)
  if recvRes.isErr:
    return err(queueError("receive MQTT worker event", recvRes.error))

  result = ok(recvRes.get())

proc joinMqttWorker*(worker: MqttWorker): MqttResult[MqttOk] =
  ## Join the worker thread.
  ##
  ## The caller should normally send a Stop command first.  This proc does not
  ## forcefully stop the worker because that would make LowLevelClient ownership
  ## ambiguous.
  if worker.isNil:
    return ok(MqttOk())
  if worker.joined:
    return ok(MqttOk())
  if not worker.started:
    return ok(MqttOk())

  joinThread(worker.thread)
  worker.joined = true
  worker.started = false
  worker.commandQueue.close()
  worker.eventQueue.close()

  result = ok(MqttOk())
