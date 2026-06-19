# Destination: src/mosquitto_nim/highlevel/dispatcher.nim

import std/[asyncdispatch, strutils]

import results

import ../lowlevel/client as lowlevel_client
import ../lowlevel/errors
import ../lowlevel/types
import ../worker/types
import ./client as highlevel_client

# ------------------------------------------------------------------------------
# High-level message dispatcher.
#
# This layer is intentionally above the worker and async bridge.  It receives
# Nim-owned MqttEvent/MqttMessage values on the asyncdispatch/application thread
# and invokes application handlers there.  libmosquitto callbacks and worker
# threads must never call these handlers directly.
# ------------------------------------------------------------------------------
type
  MqttMessageHandler* = proc(message: MqttMessage)
    ## Synchronous application message handler.
    ##
    ## Handlers are invoked by dispatchMessage()/dispatchEvent() on the
    ## asyncdispatch/application thread, not from libmosquitto callbacks.

  MqttAsyncMessageHandler* = proc(message: MqttMessage): Future[void]
    ## Async application message handler.
    ##
    ## dispatchMessage() awaits these handlers sequentially by default.  This
    ## keeps application state single-threaded and avoids implicit callback
    ## fan-out races.

  HandlerKind = enum
    hkSync
    hkAsync

  HandlerEntry = object
    id: int
    topicFilter: string
    kind: HandlerKind
    syncHandler: MqttMessageHandler
    asyncHandler: MqttAsyncMessageHandler

  MqttDispatcher* = ref object
    nextHandlerId: int
    handlers: seq[HandlerEntry]

# ------------------------------------------------------------------------------
# Construction
# ------------------------------------------------------------------------------
proc newMqttDispatcher*(): MqttDispatcher =
  new result
  result.nextHandlerId = 0
  result.handlers = @[]

proc handlerCount*(dispatcher: MqttDispatcher): int =
  if dispatcher.isNil:
    return 0
  result = dispatcher.handlers.len

# ------------------------------------------------------------------------------
# Topic matching
# ------------------------------------------------------------------------------
proc topicFilterMatches*(topicFilter, topic: string): bool =
  ## Return true when an MQTT subscription filter matches a topic name.
  ##
  ## This implements the MQTT wildcard rules needed by the high-level dispatcher:
  ## '+' matches one topic level, '#' matches remaining levels and must be the
  ## last filter level, and filters that do not start with '$' do not match '$'
  ## system topics.
  if topicFilter.len == 0 or topic.len == 0:
    return false

  if topic[0] == '$' and topicFilter[0] != '$':
    return false

  let filterLevels = topicFilter.split('/')
  let topicLevels = topic.split('/')

  var ti = 0
  for fi, filterLevel in filterLevels:
    if filterLevel == "#":
      return fi == filterLevels.high

    if ti >= topicLevels.len:
      return false

    if filterLevel != "+" and filterLevel != topicLevels[ti]:
      return false

    inc ti

  result = ti == topicLevels.len

# ------------------------------------------------------------------------------
# Handler registration
# ------------------------------------------------------------------------------
proc addMessageHandler*(dispatcher: MqttDispatcher; topicFilter: string;
                        handler: MqttMessageHandler): MqttResult[int] =
  if dispatcher.isNil:
    return err(invalidState("add MQTT message handler", "dispatcher is nil"))
  if handler.isNil:
    return err(invalidArgument("add MQTT message handler", "handler is nil"))

  let topicRes = lowlevel_client.validateSubscribeTopic(topicFilter)
  if topicRes.isErr:
    return err(topicRes.error)

  inc dispatcher.nextHandlerId
  if dispatcher.nextHandlerId <= 0:
    dispatcher.nextHandlerId = 1

  let id = dispatcher.nextHandlerId
  dispatcher.handlers.add(HandlerEntry(
    id: id,
    topicFilter: topicFilter,
    kind: hkSync,
    syncHandler: handler
  ))
  result = ok(id)

proc addMessageHandler*(dispatcher: MqttDispatcher; topicFilter: string;
                        handler: MqttAsyncMessageHandler): MqttResult[int] =
  if dispatcher.isNil:
    return err(invalidState("add MQTT async message handler", "dispatcher is nil"))
  if handler.isNil:
    return err(invalidArgument("add MQTT async message handler", "handler is nil"))

  let topicRes = lowlevel_client.validateSubscribeTopic(topicFilter)
  if topicRes.isErr:
    return err(topicRes.error)

  inc dispatcher.nextHandlerId
  if dispatcher.nextHandlerId <= 0:
    dispatcher.nextHandlerId = 1

  let id = dispatcher.nextHandlerId
  dispatcher.handlers.add(HandlerEntry(
    id: id,
    topicFilter: topicFilter,
    kind: hkAsync,
    asyncHandler: handler
  ))
  result = ok(id)

proc removeMessageHandler*(dispatcher: MqttDispatcher; handlerId: int): MqttResult[bool] =
  if dispatcher.isNil:
    return err(invalidState("remove MQTT message handler", "dispatcher is nil"))
  if handlerId <= 0:
    return err(invalidArgument("remove MQTT message handler", "handler id must be positive"))

  for i, entry in dispatcher.handlers:
    if entry.id == handlerId:
      dispatcher.handlers.delete(i)
      return ok(true)

  result = ok(false)

proc clearMessageHandlers*(dispatcher: MqttDispatcher): MqttResult[MqttOk] =
  if dispatcher.isNil:
    return err(invalidState("clear MQTT message handlers", "dispatcher is nil"))
  dispatcher.handlers.setLen(0)
  result = ok(MqttOk())

# ------------------------------------------------------------------------------
# Dispatch
# ------------------------------------------------------------------------------
proc dispatchMessage*(dispatcher: MqttDispatcher;
                      message: MqttMessage): Future[MqttResult[int]] {.async.} =
  ## Dispatch one message to matching handlers.
  ##
  ## Matching handlers are invoked sequentially.  This is deliberate: the
  ## dispatcher is meant to preserve asyncdispatch-style single-threaded
  ## application semantics.  Parallel fan-out can be added later as an explicit
  ## opt-in API if needed.
  if dispatcher.isNil:
    return err(invalidState("dispatch MQTT message", "dispatcher is nil"))

  var count = 0
  for entry in dispatcher.handlers:
    if not topicFilterMatches(entry.topicFilter, message.topic):
      continue

    try:
      case entry.kind
      of hkSync:
        entry.syncHandler(message)
      of hkAsync:
        await entry.asyncHandler(message)
    except CatchableError as e:
      return err(invalidState("dispatch MQTT message", e.msg))

    inc count

  result = ok(count)

proc dispatchEvent*(dispatcher: MqttDispatcher;
                    event: MqttEvent): Future[MqttResult[int]] {.async.} =
  ## Dispatch a worker event.
  ##
  ## Only MessageReceived events are handled here.  Control events remain visible
  ## to the caller through the normal client event stream.
  if event.kind != mevMessageReceived:
    return ok(0)

  return await dispatcher.dispatchMessage(event.message)

proc dispatchDrainedEvents*(dispatcher: MqttDispatcher;
                            client: MqttClient): Future[MqttResult[int]] {.async.} =
  ## Drain currently queued client events and dispatch all received messages.
  ##
  ## This is a convenience helper for applications/tests that poll the highlevel
  ## client.  It does not wait for new events.
  if client.isNil:
    return err(invalidState("dispatch drained MQTT events", "client is nil"))

  let drainRes = client.drainEvents()
  if drainRes.isErr:
    return err(drainRes.error)

  var count = 0
  for event in drainRes.get():
    let dispatchRes = await dispatcher.dispatchEvent(event)
    if dispatchRes.isErr:
      return err(dispatchRes.error)
    count += dispatchRes.get()

  result = ok(count)
