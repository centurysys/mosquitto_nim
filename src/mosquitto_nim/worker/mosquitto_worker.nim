# Destination: src/mosquitto_nim/worker/mosquitto_worker.nim

import std/[os, options, tables, times, typedthreads]

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

  WorkerReconnectState = object
    policy: MqttReconnectPolicy
    offlineQueuePolicy: MqttOfflineQueuePolicy
    lastConnectCommand: Option[MqttCommand]
    scheduled: bool
    reconnectAtMs: int64
    attempt: int
    explicitDisconnectRequested: bool
    reconnectApiAvailable: bool

  OfflinePublishItem = object
    command: MqttCommand
    bytes: int

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

proc sendWorkerState(eventQueue: ThreadQueue[MqttEvent]; state: MqttConnectionState;
                     commandId = 0; detail = "") {.raises: [].} =
  sendWorkerEvent(eventQueue, stateChangedEvent(state, commandId = commandId, detail = detail))

proc currentPendingOperations(pendingPublishes: Table[int, int];
                              pendingSubscribes: Table[int, int];
                              pendingUnsubscribes: Table[int, int]): MqttPendingOperations =
  result = pendingOperations(
    publishes = pendingPublishes.len,
    subscribes = pendingSubscribes.len,
    unsubscribes = pendingUnsubscribes.len
  )

proc sendWorkerPending(eventQueue: ThreadQueue[MqttEvent];
                       pendingPublishes: Table[int, int];
                       pendingSubscribes: Table[int, int];
                       pendingUnsubscribes: Table[int, int];
                       commandId = 0) {.raises: [].} =
  sendWorkerEvent(
    eventQueue,
    pendingChangedEvent(
      currentPendingOperations(pendingPublishes, pendingSubscribes, pendingUnsubscribes),
      commandId = commandId
    )
  )

proc offlineQueueBytes(offlineQueue: seq[OfflinePublishItem]): int {.raises: [].} =
  for item in offlineQueue:
    result += item.bytes

proc currentQueueSnapshot(pendingPublishes: Table[int, int];
                          pendingSubscribes: Table[int, int];
                          pendingUnsubscribes: Table[int, int];
                          offlineQueue: seq[OfflinePublishItem]): MqttQueueSnapshot =
  result = queueSnapshot(
    pending = currentPendingOperations(pendingPublishes, pendingSubscribes, pendingUnsubscribes),
    offlineQueued = offlineQueue.len,
    offlineBytes = offlineQueue.offlineQueueBytes()
  )

proc sendWorkerQueue(eventQueue: ThreadQueue[MqttEvent];
                     pendingPublishes: Table[int, int];
                     pendingSubscribes: Table[int, int];
                     pendingUnsubscribes: Table[int, int];
                     offlineQueue: seq[OfflinePublishItem];
                     commandId = 0) {.raises: [].} =
  sendWorkerEvent(
    eventQueue,
    queueChangedEvent(
      currentQueueSnapshot(pendingPublishes, pendingSubscribes, pendingUnsubscribes, offlineQueue),
      commandId = commandId
    )
  )

proc clearPendingOperations(pendingPublishes: var Table[int, int];
                            pendingSubscribes: var Table[int, int];
                            pendingUnsubscribes: var Table[int, int]) =
  pendingPublishes.clear()
  pendingSubscribes.clear()
  pendingUnsubscribes.clear()

proc nowMs(): int64 {.raises: [].} =
  result = (epochTime() * 1000.0).int64

proc cancelReconnect(reconnect: var WorkerReconnectState) {.raises: [].} =
  reconnect.scheduled = false
  reconnect.reconnectAtMs = 0

proc resetReconnectAttempt(reconnect: var WorkerReconnectState) {.raises: [].} =
  reconnect.attempt = 0
  reconnect.cancelReconnect()

proc canScheduleReconnect(reconnect: WorkerReconnectState): bool {.raises: [].} =
  result = reconnect.policy.enabled and reconnect.lastConnectCommand.isSome() and
           not reconnect.explicitDisconnectRequested

proc reconnectCommandId(reconnect: WorkerReconnectState): int {.raises: [].} =
  if reconnect.lastConnectCommand.isSome():
    return reconnect.lastConnectCommand.get().id
  result = 0

proc scheduleReconnect(reconnect: var WorkerReconnectState;
                       eventQueue: ThreadQueue[MqttEvent];
                       detail = ""): bool {.raises: [].} =
  ## Schedule the next automatic reconnect attempt using exponential backoff.
  if not reconnect.canScheduleReconnect():
    return false

  inc reconnect.attempt
  let delayMs = reconnect.policy.reconnectDelayMs(reconnect.attempt)
  reconnect.reconnectAtMs = nowMs() + delayMs.int64
  reconnect.scheduled = true

  let commandId = reconnect.reconnectCommandId()
  sendWorkerState(eventQueue, mcsReconnecting, commandId = commandId, detail = detail)
  sendWorkerEvent(
    eventQueue,
    reconnectScheduledEvent(
      delayMs,
      reconnect.attempt,
      commandId = commandId,
      detail = detail
    )
  )
  result = true

proc applyConnectConfig(client: LowLevelClient; cmd: MqttCommand): MqttResult[MqttOk] =
  ## Apply settings that libmosquitto requires before a connect attempt.
  let protoRes = setProtocolVersion(client, cmd.protocolVersion)
  if protoRes.isErr:
    return err(protoRes.error)

  let tlsRes = setTls(client, cmd.tls)
  if tlsRes.isErr:
    return err(tlsRes.error)

  let authRes = setUsernamePassword(client, cmd.username, cmd.password)
  if authRes.isErr:
    return err(authRes.error)

  let willRes = if cmd.will.enabled:
      setWill(client, cmd.will.topic, cmd.will.payload, cmd.will.qos, cmd.will.retain)
    else:
      clearWill(client)
  if willRes.isErr:
    return err(willRes.error)

  result = ok(MqttOk())

proc startConnectUsingCommand(client: LowLevelClient; cmd: MqttCommand): MqttResult[MqttOk] =
  ## Start a connect attempt using the protocol/properties carried by the command.
  if cmd.protocolVersion == mpv5:
    return connectLowLevelClientV5(
      client,
      cmd.host,
      cmd.port,
      cmd.keepalive,
      cmd.connectProperties
    )

  result = connectLowLevelClient(client, cmd.host, cmd.port, cmd.keepalive)

proc publishCommandBytes(cmd: MqttCommand): int {.raises: [].} =
  ## Approximate the memory retained by an offline publish command.
  ##
  ## This is intentionally conservative and Nim-owned. It is used only for the
  ## offline queue limit; libmosquitto packet encoding overhead is not included.
  result = cmd.topic.len + cmd.payload.len
  for property in cmd.properties:
    result += property.name.len
    result += property.value.len
    result += property.data.len
    result += 12

proc offlineQueueFits(policy: MqttOfflineQueuePolicy;
                      offlineQueue: seq[OfflinePublishItem];
                      bytes: int): bool {.raises: [].} =
  result = offlineQueue.len < policy.maxMessages and
           offlineQueue.offlineQueueBytes() + bytes <= policy.maxBytes

proc dropOldestQos0(offlineQueue: var seq[OfflinePublishItem];
                    eventQueue: ThreadQueue[MqttEvent]): bool {.raises: [].} =
  ## Drop one queued QoS0 publish and report that command as an error event.
  for i in 0 ..< offlineQueue.len:
    if offlineQueue[i].command.qos == qos0:
      let droppedCommandId = offlineQueue[i].command.id
      offlineQueue.delete(i)
      sendWorkerError(
        eventQueue,
        invalidState("MQTT offline publish queue", "queued QoS0 publish was dropped by policy"),
        droppedCommandId
      )
      return true
  result = false

proc sendPublishNow(command: sink MqttCommand;
                    client: LowLevelClient;
                    eventQueue: ThreadQueue[MqttEvent];
                    pendingPublishes: var Table[int, int];
                    pendingSubscribes: Table[int, int];
                    pendingUnsubscribes: Table[int, int]): bool {.raises: [].} =
  ## Submit a publish command to libmosquitto immediately.
  var cmd = command
  let publishRes = if cmd.properties.len > 0:
      publishLowLevelClientV5(
        client,
        cmd.topic,
        cmd.payload,
        cmd.qos,
        cmd.retain,
        cmd.properties
      )
    else:
      publishLowLevelClient(client, cmd.topic, cmd.payload, cmd.qos, cmd.retain)

  if publishRes.isErr:
    sendWorkerError(eventQueue, publishRes.error, cmd.id)
    return false

  let mid = publishRes.get()
  if mid != 0:
    pendingPublishes[mid] = cmd.id
    sendWorkerPending(eventQueue, pendingPublishes, pendingSubscribes, pendingUnsubscribes, commandId = cmd.id)
  sendWorkerEvent(eventQueue, publishAcceptedEvent(mid, commandId = cmd.id))
  result = true

proc enqueueOfflinePublish(command: sink MqttCommand;
                           policy: MqttOfflineQueuePolicy;
                           offlineQueue: var seq[OfflinePublishItem];
                           eventQueue: ThreadQueue[MqttEvent];
                           pendingPublishes: Table[int, int];
                           pendingSubscribes: Table[int, int];
                           pendingUnsubscribes: Table[int, int]) {.raises: [].} =
  ## Queue a disconnected publish according to the offline queue policy.
  var cmd = command
  if not policy.enabled:
    sendWorkerError(eventQueue, invalidState("MQTT worker publish", "client is not connected"), cmd.id)
    return

  if cmd.qos == qos0 and policy.qos0Policy == moqReject:
    sendWorkerError(
      eventQueue,
      invalidState("MQTT offline publish queue", "offline QoS0 publish is rejected by policy"),
      cmd.id
    )
    return

  let bytes = publishCommandBytes(cmd)
  if cmd.qos == qos0 and policy.qos0Policy == moqDropNewest and
      not policy.offlineQueueFits(offlineQueue, bytes):
    sendWorkerError(
      eventQueue,
      invalidState("MQTT offline publish queue", "offline QoS0 publish was dropped by policy"),
      cmd.id
    )
    return

  while not policy.offlineQueueFits(offlineQueue, bytes):
    if policy.qos0Policy == moqDropOldest and offlineQueue.dropOldestQos0(eventQueue):
      sendWorkerQueue(
        eventQueue,
        pendingPublishes,
        pendingSubscribes,
        pendingUnsubscribes,
        offlineQueue,
        commandId = cmd.id
      )
    else:
      sendWorkerError(
        eventQueue,
        invalidState("MQTT offline publish queue", "offline queue limits would be exceeded"),
        cmd.id
      )
      return

  offlineQueue.add(OfflinePublishItem(command: cmd, bytes: bytes))
  sendWorkerQueue(
    eventQueue,
    pendingPublishes,
    pendingSubscribes,
    pendingUnsubscribes,
    offlineQueue,
    commandId = cmd.id
  )

proc flushOfflinePublishes(offlineQueue: var seq[OfflinePublishItem];
                           loopActive: var bool;
                           client: LowLevelClient;
                           eventQueue: ThreadQueue[MqttEvent];
                           pendingPublishes: var Table[int, int];
                           pendingSubscribes: Table[int, int];
                           pendingUnsubscribes: Table[int, int]) {.raises: [].} =
  ## Submit queued offline publishes after a successful connection.
  while loopActive and offlineQueue.len > 0:
    var cmd = offlineQueue[0].command
    let commandId = cmd.id
    offlineQueue.delete(0)
    sendWorkerQueue(
      eventQueue,
      pendingPublishes,
      pendingSubscribes,
      pendingUnsubscribes,
      offlineQueue,
      commandId = commandId
    )

    discard sendPublishNow(
      move cmd,
      client,
      eventQueue,
      pendingPublishes,
      pendingSubscribes,
      pendingUnsubscribes
    )

proc startStoredReconnect(reconnect: var WorkerReconnectState;
                          pendingConnectId: var int;
                          loopActive: var bool;
                          client: LowLevelClient;
                          eventQueue: ThreadQueue[MqttEvent]) {.raises: [].} =
  ## Start a scheduled reconnect attempt.
  if not reconnect.scheduled or reconnect.lastConnectCommand.isNone():
    return
  if nowMs() < reconnect.reconnectAtMs:
    return

  reconnect.scheduled = false
  let cmd = reconnect.lastConnectCommand.get()
  pendingConnectId = cmd.id
  sendWorkerEvent(
    eventQueue,
    reconnectAttemptEvent(
      reconnect.attempt,
      commandId = cmd.id,
      detail = "automatic reconnect attempt"
    )
  )
  sendWorkerState(eventQueue, mcsReconnecting, commandId = cmd.id, detail = "automatic reconnect attempt")

  let mustUseStoredConnect = cmd.protocolVersion == mpv5 and cmd.connectProperties.hasProperties()
  let connectRes = if reconnect.reconnectApiAvailable and not mustUseStoredConnect:
      reconnectLowLevelClient(client)
    else:
      let configRes = applyConnectConfig(client, cmd)
      if configRes.isErr:
        pendingConnectId = 0
        sendWorkerError(eventQueue, configRes.error, cmd.id)
        discard scheduleReconnect(reconnect, eventQueue, detail = "reconnect config failed")
        return
      startConnectUsingCommand(client, cmd)

  if connectRes.isErr:
    pendingConnectId = 0
    loopActive = false
    sendWorkerError(eventQueue, connectRes.error, cmd.id)
    discard scheduleReconnect(reconnect, eventQueue, detail = "reconnect attempt failed")
    return

  reconnect.reconnectApiAvailable = true
  loopActive = true

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
                   reconnect: var WorkerReconnectState;
                   offlineQueue: var seq[OfflinePublishItem];
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
    reconnect.explicitDisconnectRequested = true
    reconnect.cancelReconnect()
    sendWorkerState(eventQueue, mcsStopping, commandId = cmd.id, detail = "stop requested")
    if loopActive:
      discard disconnectLowLevelClient(client)
      flushLoop(client, eventQueue, loopActive, loopTimeoutMs, rounds = 2)
      loopActive = false
    clearPendingOperations(pendingPublishes, pendingSubscribes, pendingUnsubscribes)
    offlineQueue.setLen(0)
    sendWorkerPending(eventQueue, pendingPublishes, pendingSubscribes, pendingUnsubscribes, commandId = cmd.id)
    sendWorkerQueue(eventQueue, pendingPublishes, pendingSubscribes, pendingUnsubscribes, offlineQueue, commandId = cmd.id)
    running = false
    sendWorkerState(eventQueue, mcsStopped, commandId = cmd.id, detail = "worker stopped")
    sendWorkerEvent(eventQueue, stoppedEvent(commandId = cmd.id))

  of mckConnect:
    if loopActive:
      sendWorkerError(
        eventQueue,
        invalidState("MQTT worker connect", "client is already connected or connecting"),
        cmd.id
      )
      return

    let reconnectRes = validateReconnectPolicy(cmd.reconnectPolicy, "MQTT worker reconnect policy")
    if reconnectRes.isErr:
      pendingConnectId = 0
      sendWorkerError(eventQueue, reconnectRes.error, cmd.id)
      return

    let offlineQueueRes = validateOfflineQueuePolicy(cmd.offlineQueuePolicy, "MQTT worker offline queue policy")
    if offlineQueueRes.isErr:
      pendingConnectId = 0
      sendWorkerError(eventQueue, offlineQueueRes.error, cmd.id)
      return

    if cmd.protocolVersion != mpv5 and cmd.connectProperties.hasProperties():
      pendingConnectId = 0
      sendWorkerError(
        eventQueue,
        invalidArgument("MQTT worker connect", "CONNECT properties require MQTT 5"),
        cmd.id
      )
      return

    let connectPropsRes = validateConnectProperties(cmd.connectProperties, "MQTT worker connect properties")
    if connectPropsRes.isErr:
      pendingConnectId = 0
      sendWorkerError(eventQueue, connectPropsRes.error, cmd.id)
      return

    reconnect.policy = cmd.reconnectPolicy
    reconnect.offlineQueuePolicy = cmd.offlineQueuePolicy
    reconnect.lastConnectCommand = some(cmd)
    reconnect.explicitDisconnectRequested = false
    reconnect.reconnectApiAvailable = false
    reconnect.resetReconnectAttempt()

    let configRes = applyConnectConfig(client, cmd)
    if configRes.isErr:
      pendingConnectId = 0
      sendWorkerError(eventQueue, configRes.error, cmd.id)
      return

    pendingConnectId = cmd.id
    sendWorkerState(eventQueue, mcsConnecting, commandId = cmd.id, detail = "connect requested")
    let connectRes = startConnectUsingCommand(client, cmd)
    if connectRes.isErr:
      pendingConnectId = 0
      sendWorkerError(eventQueue, connectRes.error, cmd.id)
      discard scheduleReconnect(reconnect, eventQueue, detail = "connect attempt failed")
      return

    reconnect.reconnectApiAvailable = true
    loopActive = true
    flushLoop(client, eventQueue, loopActive, loopTimeoutMs, rounds = 3)
    if not loopActive:
      discard scheduleReconnect(reconnect, eventQueue, detail = "connect loop failed")

  of mckDisconnect:
    reconnect.explicitDisconnectRequested = true
    reconnect.cancelReconnect()
    if offlineQueue.len > 0:
      offlineQueue.setLen(0)
      sendWorkerQueue(eventQueue, pendingPublishes, pendingSubscribes, pendingUnsubscribes, offlineQueue, commandId = cmd.id)
    if not loopActive:
      sendWorkerState(eventQueue, mcsDisconnected, commandId = cmd.id, detail = "already disconnected")
      sendWorkerEvent(eventQueue, disconnectedEvent(commandId = cmd.id, detail = "already disconnected"))
      return

    pendingDisconnectId = cmd.id
    sendWorkerState(eventQueue, mcsDisconnecting, commandId = cmd.id, detail = "disconnect requested")
    let disconnectRes = disconnectLowLevelClient(client)
    if disconnectRes.isErr:
      pendingDisconnectId = 0
      sendWorkerError(eventQueue, disconnectRes.error, cmd.id)
      loopActive = false
      return

    flushLoop(client, eventQueue, loopActive, loopTimeoutMs, rounds = 2)

  of mckPublish:
    if not loopActive:
      enqueueOfflinePublish(
        move cmd,
        reconnect.offlineQueuePolicy,
        offlineQueue,
        eventQueue,
        pendingPublishes,
        pendingSubscribes,
        pendingUnsubscribes
      )
      return

    discard sendPublishNow(
      move cmd,
      client,
      eventQueue,
      pendingPublishes,
      pendingSubscribes,
      pendingUnsubscribes
    )

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
      sendWorkerPending(eventQueue, pendingPublishes, pendingSubscribes, pendingUnsubscribes, commandId = cmd.id)

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
      sendWorkerPending(eventQueue, pendingPublishes, pendingSubscribes, pendingUnsubscribes, commandId = cmd.id)

proc workerMain(args: MqttWorkerArgs) {.thread.} =
  var stopCommandId = 0

  sendWorkerState(args.eventQueue, mcsDisconnected, detail = "worker started")

  let initRes = initLibrary()
  if initRes.isErr:
    sendWorkerError(args.eventQueue, initRes.error)
    sendWorkerState(args.eventQueue, mcsStopped, commandId = stopCommandId, detail = "worker stopped after init failure")
    sendWorkerEvent(args.eventQueue, stoppedEvent(commandId = stopCommandId))
    return

  let clientRes = newLowLevelClient(args.clientId, args.cleanSession)
  if clientRes.isErr:
    sendWorkerError(args.eventQueue, clientRes.error)
    discard cleanupLibrary()
    sendWorkerState(args.eventQueue, mcsStopped, commandId = stopCommandId, detail = "worker stopped after client creation failure")
    sendWorkerEvent(args.eventQueue, stoppedEvent(commandId = stopCommandId))
    return

  let client = clientRes.get()
  var loopActive = false
  var running = true
  var pendingConnectId = 0
  var pendingDisconnectId = 0
  var reconnect = WorkerReconnectState(
    policy: noReconnect(),
    offlineQueuePolicy: noOfflineQueue()
  )
  var pendingPublishes = initTable[int, int]()
  var pendingSubscribes = initTable[int, int]()
  var pendingUnsubscribes = initTable[int, int]()
  var offlineQueue: seq[OfflinePublishItem] = @[]

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
        reconnect.explicitDisconnectRequested = false
        reconnect.reconnectApiAvailable = true
        reconnect.resetReconnectAttempt()
        sendWorkerState(args.eventQueue, mcsConnected, commandId = commandId, detail = "connect callback")
        sendWorkerEvent(
          args.eventQueue,
          connectedEvent(commandId = commandId, reasonCode = event.reasonCode, flags = event.flags)
        )
        flushOfflinePublishes(
          offlineQueue,
          loopActive,
          client,
          args.eventQueue,
          pendingPublishes,
          pendingSubscribes,
          pendingUnsubscribes
        )
      else:
        loopActive = false
        sendWorkerState(args.eventQueue, mcsError, commandId = commandId, detail = "connect rejected")
        sendWorkerError(args.eventQueue, protocolReason("MQTT connect callback", event.reasonCode), commandId)

    of lleDisconnected:
      let explicit = reconnect.explicitDisconnectRequested or pendingDisconnectId != 0
      let commandId = if pendingDisconnectId != 0: pendingDisconnectId else: reconnect.reconnectCommandId()
      pendingDisconnectId = 0
      pendingConnectId = 0
      loopActive = false
      clearPendingOperations(pendingPublishes, pendingSubscribes, pendingUnsubscribes)
      sendWorkerPending(args.eventQueue, pendingPublishes, pendingSubscribes, pendingUnsubscribes, commandId = commandId)
      sendWorkerQueue(args.eventQueue, pendingPublishes, pendingSubscribes, pendingUnsubscribes, offlineQueue, commandId = commandId)
      sendWorkerState(args.eventQueue, mcsDisconnected, commandId = commandId, detail = "disconnect callback")
      sendWorkerEvent(
        args.eventQueue,
        disconnectedEvent(commandId = commandId, reasonCode = event.reasonCode)
      )
      if not explicit:
        discard scheduleReconnect(reconnect, args.eventQueue, detail = "unexpected disconnect")

    of llePublishCompleted:
      let commandId = pendingPublishes.getOrDefault(event.mid, 0)
      if event.mid in pendingPublishes:
        pendingPublishes.del(event.mid)
        sendWorkerPending(args.eventQueue, pendingPublishes, pendingSubscribes, pendingUnsubscribes, commandId = commandId)
      sendWorkerEvent(
        args.eventQueue,
        publishCompletedEvent(event.mid, commandId = commandId, reasonCode = event.reasonCode)
      )

    of lleSubscribed:
      let commandId = pendingSubscribes.getOrDefault(event.mid, 0)
      if event.mid in pendingSubscribes:
        pendingSubscribes.del(event.mid)
        sendWorkerPending(args.eventQueue, pendingPublishes, pendingSubscribes, pendingUnsubscribes, commandId = commandId)
      sendWorkerEvent(
        args.eventQueue,
        subscribedEvent(event.mid, commandId = commandId, grantedQos = event.grantedQos)
      )

    of lleUnsubscribed:
      let commandId = pendingUnsubscribes.getOrDefault(event.mid, 0)
      if event.mid in pendingUnsubscribes:
        pendingUnsubscribes.del(event.mid)
        sendWorkerPending(args.eventQueue, pendingPublishes, pendingSubscribes, pendingUnsubscribes, commandId = commandId)
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
        reconnect,
        offlineQueue,
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
          pendingConnectId = 0
          clearPendingOperations(pendingPublishes, pendingSubscribes, pendingUnsubscribes)
          sendWorkerPending(args.eventQueue, pendingPublishes, pendingSubscribes, pendingUnsubscribes)
          sendWorkerQueue(args.eventQueue, pendingPublishes, pendingSubscribes, pendingUnsubscribes, offlineQueue)
          sendWorkerError(args.eventQueue, loopRes.error)
          if not scheduleReconnect(reconnect, args.eventQueue, detail = "mosquitto loop error"):
            sendWorkerState(args.eventQueue, mcsError, detail = "mosquitto loop error")
      else:
        if reconnect.scheduled and nowMs() >= reconnect.reconnectAtMs:
          startStoredReconnect(reconnect, pendingConnectId, loopActive, client, args.eventQueue)
        else:
          sleep(args.idleSleepMs)

    let callbackError = lastCallbackError(client)
    if callbackError.isSome():
      sendWorkerError(args.eventQueue, callbackError.get())
      discard clearCallbackError(client)

  if loopActive:
    discard disconnectLowLevelClient(client)
    loopActive = false

  clearPendingOperations(pendingPublishes, pendingSubscribes, pendingUnsubscribes)
  offlineQueue.setLen(0)

  if running:
    sendWorkerState(args.eventQueue, mcsStopped, commandId = stopCommandId, detail = "worker stopped after loop exit")

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
