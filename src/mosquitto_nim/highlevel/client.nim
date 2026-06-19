# Destination: src/mosquitto_nim/highlevel/client.nim

import std/asyncdispatch

import results

import ../lowlevel/errors
import ../lowlevel/types
import ../worker/types
import ../worker/mosquitto_worker
import ./async_bridge
import ./dispatcher

# ------------------------------------------------------------------------------
# High-level asyncdispatch MQTT client.
#
# This layer owns the worker + async bridge pair and exposes application-facing
# operations that do not require callers to touch threadtools queues directly.
#
# Operation methods such as publish/subscribe/connect enqueue commands into the
# worker and return the command id.  They do not wait for broker callbacks.  This
# keeps nmqtt-compatible publish semantics possible: publish can complete once
# the command is accepted by the local worker queue, while PublishCompleted
# remains available as a separate event for callers that need PUBACK/PUBCOMP
# tracking.
# ------------------------------------------------------------------------------
type
  MqttSubscription* = object
    ## Handler + broker subscription pair returned by subscribe(..., handler).
    ##
    ## commandId is the worker command id for the broker SUBSCRIBE command.
    ## handlerId is the highlevel dispatcher handler id.  They are intentionally
    ## separate because broker subscription acknowledgement and application
    ## callback registration have different lifetimes.
    commandId*: int
    handlerId*: int
    topicFilter*: string

  MqttClient* = ref object
    worker: MqttWorker
    bridge: MqttAsyncBridge
    dispatcher: MqttDispatcher
    nextCommandId: int
    state: MqttConnectionState
    pending: MqttPendingOperations
    reconnectPolicy: MqttReconnectPolicy
    offlineQueuePolicy: MqttOfflineQueuePolicy
    closed: bool

# ------------------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------------------
proc ensureOpen(client: MqttClient; context: string): MqttResult[MqttOk] =
  if client.isNil:
    return err(invalidState(context, "client is nil"))
  if client.closed:
    return err(invalidState(context, "client is closed"))
  if client.worker.isNil or not client.worker.isStarted:
    return err(invalidState(context, "worker is not started"))
  if client.bridge.isNil or client.bridge.isClosed:
    return err(invalidState(context, "async bridge is closed"))

  result = ok(MqttOk())

proc updateStateFromEvent(client: MqttClient; event: MqttEvent) =
  if client.isNil:
    return

  case event.kind
  of mevStateChanged:
    client.state = event.state
  of mevPendingChanged:
    client.pending = event.pending
  of mevConnected:
    client.state = mcsConnected
  of mevDisconnected:
    client.state = mcsDisconnected
    client.pending = emptyPendingOperations()
  of mevReconnectScheduled, mevReconnectAttempt:
    client.state = mcsReconnecting
  of mevStopped:
    client.state = mcsStopped
    client.pending = emptyPendingOperations()
  else:
    discard

proc allocateCommandId(client: MqttClient): int =
  ## Allocate a positive command id for correlating command-side results/events.
  inc client.nextCommandId
  if client.nextCommandId <= 0:
    client.nextCommandId = 1
  result = client.nextCommandId

proc sendClientCommand(client: MqttClient; command: sink MqttCommand;
                       context: string): MqttResult[int] =
  let openRes = client.ensureOpen(context)
  if openRes.isErr:
    return err(openRes.error)

  var cmd = command
  if cmd.id == 0:
    cmd.id = client.allocateCommandId()

  let commandId = cmd.id
  let sendRes = client.worker.sendCommand(move cmd)
  if sendRes.isErr:
    return err(sendRes.error)

  result = ok(commandId)

# ------------------------------------------------------------------------------
# Public construction / lifecycle API
# ------------------------------------------------------------------------------
proc startMqttClient*(clientId = ""; cleanSession = true;
                      commandQueueLen = 32; eventQueueLen = 32;
                      idleSleepMs = 1; loopTimeoutMs = 10;
                      pollMs = 1): MqttResult[MqttClient] =
  ## Start a high-level MQTT client.
  ##
  ## This starts a worker thread, creates an asyncdispatch bridge for its event
  ## queue, and returns a client object that can send commands and await events.
  if pollMs < 0:
    return err(invalidArgument("start MQTT client", "pollMs must not be negative"))

  let workerRes = startMqttWorker(
    clientId = clientId,
    cleanSession = cleanSession,
    commandQueueLen = commandQueueLen,
    eventQueueLen = eventQueueLen,
    idleSleepMs = idleSleepMs,
    loopTimeoutMs = loopTimeoutMs,
  )
  if workerRes.isErr:
    return err(workerRes.error)

  let worker = workerRes.get()
  let bridgeRes = newMqttAsyncBridge(worker, pollMs = pollMs)
  if bridgeRes.isErr:
    discard worker.requestStop()
    discard worker.joinMqttWorker()
    return err(bridgeRes.error)

  var client: MqttClient
  new client
  client.worker = worker
  client.bridge = bridgeRes.get()
  client.dispatcher = newMqttDispatcher()
  client.nextCommandId = 0
  client.state = mcsDisconnected
  client.pending = emptyPendingOperations()
  client.reconnectPolicy = noReconnect()
  client.offlineQueuePolicy = noOfflineQueue()
  client.closed = false
  result = ok(client)

proc isClosed*(client: MqttClient): bool {.inline.} =
  result = client.isNil or client.closed

proc isStarted*(client: MqttClient): bool {.inline.} =
  result = not client.isNil and not client.closed and
           not client.worker.isNil and client.worker.isStarted

proc currentState*(client: MqttClient): MqttConnectionState {.inline.} =
  if client.isNil:
    return mcsStopped
  result = client.state

proc isConnected*(client: MqttClient): bool {.inline.} =
  result = not client.isNil and client.state.isConnected()

proc pendingOperations*(client: MqttClient): MqttPendingOperations {.inline.} =
  ## Return the latest worker-reported in-flight operation snapshot.
  ##
  ## This reflects broker-level pending mids that the worker has already
  ## submitted to libmosquitto. It does not include commands that are only queued
  ## locally and not yet processed by the worker.
  if client.isNil:
    return emptyPendingOperations()
  result = client.pending

proc pendingTotal*(client: MqttClient): int {.inline.} =
  if client.isNil:
    return 0
  result = client.pending.total

proc msgQueue*(client: MqttClient): int {.inline.} =
  ## Compatibility/debug helper returning the latest pending operation total.
  result = client.pendingTotal()

proc reconnectPolicy*(client: MqttClient): MqttReconnectPolicy {.inline.} =
  ## Return the reconnect policy that will be attached to future connect commands.
  if client.isNil:
    return noReconnect()
  result = client.reconnectPolicy

proc setReconnectPolicy*(client: MqttClient;
                         policy: MqttReconnectPolicy): MqttResult[MqttOk] =
  ## Store a reconnect policy for future connect commands.
  ##
  ## Automatic reconnect attempts are scheduled by the worker after unexpected
  ## disconnects or network-loop errors. Explicit disconnect/stop still cancels
  ## reconnect scheduling.
  let openRes = client.ensureOpen("set MQTT reconnect policy")
  if openRes.isErr:
    return err(openRes.error)

  let validateRes = validateReconnectPolicy(policy, "set MQTT reconnect policy")
  if validateRes.isErr:
    return err(validateRes.error)

  client.reconnectPolicy = policy
  result = ok(MqttOk())

proc enableReconnect*(client: MqttClient; initialDelayMs = 1000;
                      maxDelayMs = 30000; multiplier = 2.0): MqttResult[MqttOk] =
  result = client.setReconnectPolicy(
    mqttReconnectPolicy(
      initialDelayMs = initialDelayMs,
      maxDelayMs = maxDelayMs,
      multiplier = multiplier
    )
  )

proc disableReconnect*(client: MqttClient): MqttResult[MqttOk] =
  result = client.setReconnectPolicy(noReconnect())


proc offlineQueuePolicy*(client: MqttClient): MqttOfflineQueuePolicy {.inline.} =
  ## Return the offline publish queue policy attached to future connect commands.
  if client.isNil:
    return noOfflineQueue()
  result = client.offlineQueuePolicy

proc setOfflineQueuePolicy*(client: MqttClient;
                            policy: MqttOfflineQueuePolicy): MqttResult[MqttOk] =
  ## Store an offline publish queue policy for future connect commands.
  ##
  ## Step 26 only wires the policy through the public API and worker connect
  ## configuration. Actual disconnected publish queueing remains disabled until
  ## a later implementation step.
  let openRes = client.ensureOpen("set MQTT offline queue policy")
  if openRes.isErr:
    return err(openRes.error)

  let validateRes = validateOfflineQueuePolicy(policy, "set MQTT offline queue policy")
  if validateRes.isErr:
    return err(validateRes.error)

  client.offlineQueuePolicy = policy
  result = ok(MqttOk())

proc enableOfflineQueue*(client: MqttClient; maxMessages = 100;
                         maxBytes = 1024 * 1024;
                         qos0Policy = moqReject): MqttResult[MqttOk] =
  result = client.setOfflineQueuePolicy(
    mqttOfflineQueuePolicy(
      maxMessages = maxMessages,
      maxBytes = maxBytes,
      qos0Policy = qos0Policy
    )
  )

proc disableOfflineQueue*(client: MqttClient): MqttResult[MqttOk] =
  result = client.setOfflineQueuePolicy(noOfflineQueue())

proc joinMqttClient*(client: MqttClient): MqttResult[MqttOk] =
  ## Join the worker thread after a Stop command has been processed.
  ##
  ## This does not send Stop by itself.  Call requestStop() first, optionally
  ## await the mevStopped event, then join.  Keeping these steps explicit makes
  ## shutdown ordering visible to applications.
  if client.isNil:
    return err(invalidState("join MQTT client", "client is nil"))

  if not client.bridge.isNil:
    client.bridge.close()

  if not client.worker.isNil:
    let joinRes = client.worker.joinMqttWorker()
    if joinRes.isErr:
      return err(joinRes.error)

  client.closed = true
  client.pending = emptyPendingOperations()
  if client.state != mcsError:
    client.state = mcsStopped
  result = ok(MqttOk())

proc mqttWorker*(client: MqttClient): MqttWorker =
  ## Return the owned worker for tests/diagnostics.
  ##
  ## Application code should normally use highlevel methods instead of touching
  ## the worker directly.
  if client.isNil:
    return nil
  result = client.worker

proc mqttDispatcher*(client: MqttClient): MqttDispatcher =
  ## Return the owned dispatcher for tests/diagnostics.
  ##
  ## Application code should normally use addMessageHandler(), subscribe(...,
  ## handler), and dispatchEvent() wrappers instead of reaching into the
  ## dispatcher directly.
  if client.isNil:
    return nil
  result = client.dispatcher

proc messageHandlerCount*(client: MqttClient): int =
  if client.isNil or client.dispatcher.isNil:
    return 0
  result = client.dispatcher.handlerCount()

# ------------------------------------------------------------------------------
# Dispatcher registration API
# ------------------------------------------------------------------------------
proc addMessageHandler*(client: MqttClient; topicFilter: string;
                        handler: MqttMessageHandler): MqttResult[int] =
  if client.isNil:
    return err(invalidState("add MQTT client message handler", "client is nil"))
  if client.closed:
    return err(invalidState("add MQTT client message handler", "client is closed"))
  if client.dispatcher.isNil:
    return err(invalidState("add MQTT client message handler", "dispatcher is nil"))

  result = client.dispatcher.addMessageHandler(topicFilter, handler)

proc addMessageHandler*(client: MqttClient; topicFilter: string;
                        handler: MqttAsyncMessageHandler): MqttResult[int] =
  if client.isNil:
    return err(invalidState("add MQTT client async message handler", "client is nil"))
  if client.closed:
    return err(invalidState("add MQTT client async message handler", "client is closed"))
  if client.dispatcher.isNil:
    return err(invalidState("add MQTT client async message handler", "dispatcher is nil"))

  result = client.dispatcher.addMessageHandler(topicFilter, handler)

proc removeMessageHandler*(client: MqttClient; handlerId: int): MqttResult[bool] =
  if client.isNil:
    return err(invalidState("remove MQTT client message handler", "client is nil"))
  if client.closed:
    return err(invalidState("remove MQTT client message handler", "client is closed"))
  if client.dispatcher.isNil:
    return err(invalidState("remove MQTT client message handler", "dispatcher is nil"))

  result = client.dispatcher.removeMessageHandler(handlerId)

proc clearMessageHandlers*(client: MqttClient): MqttResult[MqttOk] =
  if client.isNil:
    return err(invalidState("clear MQTT client message handlers", "client is nil"))
  if client.closed:
    return err(invalidState("clear MQTT client message handlers", "client is closed"))
  if client.dispatcher.isNil:
    return err(invalidState("clear MQTT client message handlers", "dispatcher is nil"))

  result = client.dispatcher.clearMessageHandlers()

# ------------------------------------------------------------------------------
# Command API
# ------------------------------------------------------------------------------
proc sendCommand*(client: MqttClient; command: sink MqttCommand): MqttResult[int] =
  ## Send a raw worker command through the highlevel client.
  ##
  ## The returned value is the command id assigned to this command.  Success means
  ## the command was queued locally, not that the broker has acknowledged it.
  result = client.sendClientCommand(move command, "send MQTT client command")

proc connect*(client: MqttClient; host: string; port = 1883;
              keepalive = 60; protocolVersion = mpv311;
              username = ""; password = "";
              tls: MqttTlsConfig = MqttTlsConfig(enabled: false);
              will: MqttWill = MqttWill(enabled: false, qos: qos0)): MqttResult[int] =
  let policy = if client.isNil: noReconnect() else: client.reconnectPolicy
  let offlinePolicy = if client.isNil: noOfflineQueue() else: client.offlineQueuePolicy
  var cmd = connectCommand(
    host,
    port = port,
    keepalive = keepalive,
    protocolVersion = protocolVersion,
    username = username,
    password = password,
    tls = tls,
    will = will,
    reconnectPolicy = policy,
    offlineQueuePolicy = offlinePolicy
  )
  result = client.sendClientCommand(move cmd, "connect MQTT client")
  if result.isOk:
    client.state = mcsConnecting

proc disconnect*(client: MqttClient): MqttResult[int] =
  var cmd = disconnectCommand()
  result = client.sendClientCommand(move cmd, "disconnect MQTT client")
  if result.isOk:
    client.state = mcsDisconnecting

proc requestStop*(client: MqttClient): MqttResult[int] =
  var cmd = stopCommand()
  result = client.sendClientCommand(move cmd, "stop MQTT client")
  if result.isOk:
    client.state = mcsStopping

proc publish*(client: MqttClient; topic: string; payload: openArray[byte];
              qos = qos0; retain = false): MqttResult[int] =
  ## Queue a PUBLISH command.
  ##
  ## This does not wait for PublishCompleted/PUBACK.  Callers that need
  ## completion tracking should watch mevPublishAccepted/mevPublishCompleted
  ## events using the returned command id.
  var cmd = publishCommand(topic, payload, qos = qos, retain = retain)
  result = client.sendClientCommand(move cmd, "publish MQTT message")

proc publish*(client: MqttClient; topic: string; payload: string;
              qos = qos0; retain = false): MqttResult[int] =
  var cmd = publishCommand(topic, payload, qos = qos, retain = retain)
  result = client.sendClientCommand(move cmd, "publish MQTT message")

proc publishV5*(client: MqttClient; topic: string; payload: openArray[byte];
                qos = qos0; retain = false;
                properties: MqttProperties = @[]): MqttResult[int] =
  ## Queue a PUBLISH command with MQTT v5 properties.
  ##
  ## This is still queue-oriented. It does not wait for PublishCompleted/PUBACK.
  var cmd = publishV5Command(topic, payload, qos = qos, retain = retain, properties = properties)
  result = client.sendClientCommand(move cmd, "publish MQTT v5 message")

proc publishV5*(client: MqttClient; topic: string; payload: string;
                qos = qos0; retain = false;
                properties: MqttProperties = @[]): MqttResult[int] =
  var cmd = publishV5Command(topic, payload, qos = qos, retain = retain, properties = properties)
  result = client.sendClientCommand(move cmd, "publish MQTT v5 message")

proc subscribe*(client: MqttClient; topicFilter: string; qos = qos0): MqttResult[int] =
  var cmd = subscribeCommand(topicFilter, qos = qos)
  result = client.sendClientCommand(move cmd, "subscribe MQTT topic")

proc subscribe*(client: MqttClient; topicFilter: string; qos: MqttQos;
                handler: MqttMessageHandler): MqttResult[MqttSubscription] =
  ## Register a sync handler and queue a broker SUBSCRIBE command.
  ##
  ## Success means the handler is registered locally and the SUBSCRIBE command
  ## was accepted by the worker queue.  It does not wait for SUBACK.  Watch the
  ## mevSubscribed event with subscription.commandId when broker acknowledgement
  ## matters.
  let handlerRes = client.addMessageHandler(topicFilter, handler)
  if handlerRes.isErr:
    return err(handlerRes.error)

  let commandRes = client.subscribe(topicFilter, qos = qos)
  if commandRes.isErr:
    discard client.removeMessageHandler(handlerRes.get())
    return err(commandRes.error)

  result = ok(MqttSubscription(
    commandId: commandRes.get(),
    handlerId: handlerRes.get(),
    topicFilter: topicFilter
  ))

proc subscribe*(client: MqttClient; topicFilter: string; qos: MqttQos;
                handler: MqttAsyncMessageHandler): MqttResult[MqttSubscription] =
  ## Register an async handler and queue a broker SUBSCRIBE command.
  ##
  ## Async handlers are awaited serially by dispatchEvent()/dispatchDrainedEvents().
  let handlerRes = client.addMessageHandler(topicFilter, handler)
  if handlerRes.isErr:
    return err(handlerRes.error)

  let commandRes = client.subscribe(topicFilter, qos = qos)
  if commandRes.isErr:
    discard client.removeMessageHandler(handlerRes.get())
    return err(commandRes.error)

  result = ok(MqttSubscription(
    commandId: commandRes.get(),
    handlerId: handlerRes.get(),
    topicFilter: topicFilter
  ))

proc unsubscribe*(client: MqttClient; topicFilter: string): MqttResult[int] =
  var cmd = unsubscribeCommand(topicFilter)
  result = client.sendClientCommand(move cmd, "unsubscribe MQTT topic")

proc unsubscribe*(client: MqttClient; subscription: MqttSubscription): MqttResult[int] =
  ## Remove the registered handler and queue a broker UNSUBSCRIBE command.
  ##
  ## The local handler is removed before UNSUBSCRIBE is queued, so no further
  ## callbacks are dispatched for this subscription even if the broker still has
  ## in-flight messages before UNSUBACK.
  if subscription.handlerId > 0:
    let removeRes = client.removeMessageHandler(subscription.handlerId)
    if removeRes.isErr:
      return err(removeRes.error)

  result = client.unsubscribe(subscription.topicFilter)

# ------------------------------------------------------------------------------
# Event API
# ------------------------------------------------------------------------------
proc nextEvent*(client: MqttClient): Future[MqttResult[MqttEvent]] {.async.} =
  ## Await the next MQTT worker event on the asyncdispatch thread.
  if client.isNil:
    return err(invalidState("MQTT client nextEvent", "client is nil"))
  if client.closed:
    return err(invalidState("MQTT client nextEvent", "client is closed"))
  if client.bridge.isNil:
    return err(invalidState("MQTT client nextEvent", "async bridge is nil"))

  let eventRes = await client.bridge.nextEvent()
  if eventRes.isOk:
    client.updateStateFromEvent(eventRes.get())
  return eventRes

proc drainEvents*(client: MqttClient): MqttResult[seq[MqttEvent]] =
  ## Drain currently queued worker events without waiting.
  if client.isNil:
    return err(invalidState("MQTT client drainEvents", "client is nil"))
  if client.closed:
    return err(invalidState("MQTT client drainEvents", "client is closed"))
  if client.bridge.isNil:
    return err(invalidState("MQTT client drainEvents", "async bridge is nil"))

  let drainRes = client.bridge.drainEvents()
  if drainRes.isErr:
    return err(drainRes.error)

  for event in drainRes.get():
    client.updateStateFromEvent(event)

  result = drainRes

# ------------------------------------------------------------------------------
# Dispatch API
# ------------------------------------------------------------------------------
proc dispatchEvent*(client: MqttClient; event: MqttEvent): Future[MqttResult[int]] {.async.} =
  ## Dispatch one worker event through the client's message dispatcher.
  ##
  ## Only mevMessageReceived invokes handlers. Control events remain visible to
  ## callers through nextEvent()/drainEvents() and return a dispatch count of 0.
  if client.isNil:
    return err(invalidState("dispatch MQTT client event", "client is nil"))
  if client.closed:
    return err(invalidState("dispatch MQTT client event", "client is closed"))
  if client.dispatcher.isNil:
    return err(invalidState("dispatch MQTT client event", "dispatcher is nil"))

  return await client.dispatcher.dispatchEvent(event)

proc dispatchDrainedEvents*(client: MqttClient): Future[MqttResult[int]] {.async.} =
  ## Drain currently queued events and dispatch all message events.
  ##
  ## This is convenient for nmqtt-style callback loops. It deliberately does not
  ## make publish wait for PUBACK; PublishAccepted/PublishCompleted still remain
  ## ordinary control events visible to callers using drainEvents()/nextEvent().
  if client.isNil:
    return err(invalidState("dispatch drained MQTT client events", "client is nil"))

  let drainRes = client.drainEvents()
  if drainRes.isErr:
    return err(drainRes.error)

  var count = 0
  for event in drainRes.get():
    let dispatchRes = await client.dispatchEvent(event)
    if dispatchRes.isErr:
      return err(dispatchRes.error)
    count += dispatchRes.get()

  result = ok(count)

proc dispatchNextEvent*(client: MqttClient): Future[MqttResult[int]] {.async.} =
  ## Await one event and dispatch it if it is a message event.
  if client.isNil:
    return err(invalidState("dispatch next MQTT client event", "client is nil"))

  let eventRes = await client.nextEvent()
  if eventRes.isErr:
    return err(eventRes.error)

  return await client.dispatchEvent(eventRes.get())
