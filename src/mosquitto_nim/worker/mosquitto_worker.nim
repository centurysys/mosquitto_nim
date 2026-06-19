# Destination: src/mosquitto_nim/worker/mosquitto_worker.nim

import std/[os, strformat, typedthreads]

import results
import threadtools

import ../lowlevel/client
import ../lowlevel/errors
import ../lowlevel/library
import ./types

# ------------------------------------------------------------------------------
# MQTT worker thread shell.
#
# This module is the first threadtools-based layer.  It owns the command/event
# queues and starts a Nim-managed worker thread.  The worker creates and destroys
# the LowLevelClient inside the worker thread, so the libmosquitto handle does not
# cross the public API boundary.
#
# This step intentionally implements only start/stop and unknown-command error
# reporting.  Network commands are wired in a later step after the worker
# lifecycle is proven stable.
# ------------------------------------------------------------------------------
type
  MqttWorkerArgs = object
    clientId: string
    cleanSession: bool
    idleSleepMs: int
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
  ## to report that from this low-level worker shell.
  if eventQueue.isNil or eventQueue.isClosed:
    return

  var ev = event
  discard eventQueue.sendMove(ev)

proc sendWorkerError(eventQueue: ThreadQueue[MqttEvent]; error: MqttError;
                     commandId = 0) {.raises: [].} =
  sendWorkerEvent(eventQueue, errorEvent(error, commandId))

proc handleCommand(command: sink MqttCommand; running: var bool;
                   eventQueue: ThreadQueue[MqttEvent]) {.raises: [].} =
  ## Handle one worker command.
  ##
  ## Step 6 only proves the worker lifecycle.  MQTT network operations are added
  ## in the next step, after command/event queue ownership is known to work.
  var cmd = command
  case cmd.kind
  of mckStop:
    running = false
    sendWorkerEvent(eventQueue, stoppedEvent(commandId = cmd.id))
  of mckConnect, mckDisconnect, mckPublish, mckSubscribe, mckUnsubscribe:
    sendWorkerError(
      eventQueue,
      invalidState("MQTT worker", &"command is not implemented yet: {cmd.kind}"),
      cmd.id
    )

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
  var running = true

  while running:
    var command: MqttCommand
    let recvRes = args.commandQueue.tryReceive(command)
    if recvRes.isErr:
      sendWorkerError(args.eventQueue, queueError("MQTT worker command queue", recvRes.error))
      break

    if recvRes.get():
      stopCommandId = command.id
      handleCommand(move command, running, args.eventQueue)
    else:
      sleep(args.idleSleepMs)

  discard closeLowLevelClient(client)
  discard cleanupLibrary()

# ------------------------------------------------------------------------------
# Public worker API
# ------------------------------------------------------------------------------
proc startMqttWorker*(clientId = ""; cleanSession = true;
                      commandQueueLen = 32; eventQueueLen = 32;
                      idleSleepMs = 1): MqttResult[MqttWorker] =
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
