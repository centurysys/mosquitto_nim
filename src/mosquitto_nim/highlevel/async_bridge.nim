# Destination: src/mosquitto_nim/highlevel/async_bridge.nim

import std/asyncdispatch

import results
import threadtools

import ../lowlevel/errors
import ../worker/types
import ../worker/mosquitto_worker

# ------------------------------------------------------------------------------
# Worker event queue -> asyncdispatch bridge.
#
# This is the first highlevel bridge.  It intentionally uses threadtools'
# polling recvAsync(queue, pollMs) because the worker currently sends directly to
# its event queue.  A later step can switch the worker to AsyncThreadQueueNotifier
# for event-based wakeups without changing the highlevel nextEvent() shape.
# ------------------------------------------------------------------------------
type
  MqttAsyncBridge* = ref object
    worker: MqttWorker
    eventQueue: ThreadQueue[MqttEvent]
    pollMs: int
    closed: bool

# ------------------------------------------------------------------------------
# Error helpers
# ------------------------------------------------------------------------------
proc queueError(context: string; code: ErrorCode): MqttError =
  case code
  of ErrorCode.Closed:
    result = makeError(meQueueClosed, context, $code, ord(code))
  of ErrorCode.Full:
    result = makeError(meQueueOverflow, context, $code, ord(code))
  else:
    result = makeError(meInvalidState, context, $code, ord(code))

proc asyncReceiveError(context: string; message: string): MqttError =
  result = invalidState(context, message)

proc takeEvent(box: AsyncOwned[MqttEvent]): MqttResult[MqttEvent] =
  if box.isNil:
    return err(invalidState("MQTT async bridge take event", "async owned box is nil"))

  var takeRes = box.take()
  if takeRes.isErr:
    return err(queueError("MQTT async bridge take event", takeRes.error))

  var event = takeRes.take()
  result = ok(event)

# ------------------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------------------
proc newMqttAsyncBridge*(worker: MqttWorker; pollMs = DefaultAsyncPollMs): MqttResult[MqttAsyncBridge] =
  ## Create an asyncdispatch bridge for a worker's event queue.
  ##
  ## The bridge must be used from the asyncdispatch/application thread.  It does
  ## not own the worker and does not join it; callers should stop/join the worker
  ## explicitly after closing the bridge.
  if worker.isNil:
    return err(invalidArgument("create MQTT async bridge", "worker is nil"))
  if not worker.isStarted:
    return err(invalidState("create MQTT async bridge", "worker is not started"))
  if pollMs < 0:
    return err(invalidArgument("create MQTT async bridge", "pollMs must not be negative"))

  let queueRes = eventQueueForAsyncBridge(worker)
  if queueRes.isErr:
    return err(queueRes.error)

  var bridge: MqttAsyncBridge
  new bridge
  bridge.worker = worker
  bridge.eventQueue = queueRes.get()
  bridge.pollMs = pollMs
  bridge.closed = false
  return ok(bridge)

proc isClosed*(bridge: MqttAsyncBridge): bool {.inline.} =
  result = bridge.isNil or bridge.closed

proc close*(bridge: MqttAsyncBridge) =
  ## Close the highlevel async bridge.
  ##
  ## This does not close worker queues and does not stop the MQTT worker.  The
  ## worker lifecycle remains explicit so shutdown ordering stays visible.
  if bridge.isNil:
    return
  bridge.closed = true

proc nextEvent*(bridge: MqttAsyncBridge): Future[MqttResult[MqttEvent]] {.async.} =
  ## Await the next worker event on the asyncdispatch thread.
  ##
  ## The worker thread never completes Future objects directly.  This proc awaits
  ## threadtools' async queue bridge from the asyncdispatch side and converts the
  ## owned event into a normal MqttResult[MqttEvent].
  if bridge.isNil:
    return err(invalidState("MQTT async bridge nextEvent", "bridge is nil"))
  if bridge.closed:
    return err(invalidState("MQTT async bridge nextEvent", "bridge is closed"))
  if bridge.eventQueue.isNil:
    return err(invalidState("MQTT async bridge nextEvent", "event queue is nil"))

  try:
    let box = await bridge.eventQueue.recvAsync(bridge.pollMs)
    return takeEvent(box)
  except CatchableError as e:
    return err(asyncReceiveError("MQTT async bridge nextEvent", e.msg))

proc drainEvents*(bridge: MqttAsyncBridge): MqttResult[seq[MqttEvent]] =
  ## Drain currently queued events without waiting.
  if bridge.isNil:
    return err(invalidState("MQTT async bridge drainEvents", "bridge is nil"))
  if bridge.closed:
    return err(invalidState("MQTT async bridge drainEvents", "bridge is closed"))
  if bridge.eventQueue.isNil:
    return err(invalidState("MQTT async bridge drainEvents", "event queue is nil"))

  var events: seq[MqttEvent] = @[]
  while true:
    var event: MqttEvent
    let recvRes = bridge.eventQueue.tryReceive(event)
    if recvRes.isErr:
      return err(queueError("MQTT async bridge drainEvents", recvRes.error))
    if not recvRes.get():
      break
    events.add(event)

  result = ok(events)

proc bridgeWorker*(bridge: MqttAsyncBridge): MqttWorker =
  ## Return the worker associated with this bridge.
  if bridge.isNil:
    return nil
  result = bridge.worker
