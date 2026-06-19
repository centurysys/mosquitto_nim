# Destination: src/mosquitto_nim/worker/types.nim

import std/strformat

import results

import ../lowlevel/errors
import ../lowlevel/types

# ------------------------------------------------------------------------------
# Worker command/event value types.
#
# These types are intentionally pure Nim data. They are meant to cross
# threadtools queues, so they must not contain libmosquitto raw pointers,
# callbacks, Future objects, or other thread-affine state.
# ------------------------------------------------------------------------------
type
  MqttCommandKind* = enum
    mckConnect
    mckDisconnect
    mckPublish
    mckSubscribe
    mckUnsubscribe
    mckStop

  MqttConnectionState* = enum
    mcsDisconnected
    mcsConnecting
    mcsConnected
    mcsDisconnecting
    mcsReconnecting
    mcsStopping
    mcsStopped
    mcsError

  MqttEventKind* = enum
    mevStateChanged
    mevPendingChanged
    mevQueueChanged
    mevConnected
    mevDisconnected
    mevPublishAccepted
    mevPublishCompleted
    mevSubscribed
    mevUnsubscribed
    mevReconnectScheduled
    mevReconnectAttempt
    mevMessageReceived
    mevError
    mevStopped

  MqttReconnectPolicy* = object
    ## Reconnect configuration carried by connect commands.
    ##
    ## Auto reconnect is disabled by default. When enabled, the worker schedules
    ## reconnect attempts only after unexpected disconnects or network-loop errors.
    enabled*: bool
    initialDelayMs*: int
    maxDelayMs*: int
    multiplier*: float

  MqttOfflineQos0Policy* = enum
    ## Policy for QoS0 publishes while the client is offline/reconnecting.
    ##
    ## QoS0 publishes may be rejected, dropped, or retained while the client is
    ## offline/reconnecting, depending on this policy.
    moqReject
    moqDropNewest
    moqDropOldest
    moqQueue

  MqttOfflineQueuePolicy* = object
    ## Offline publish queue configuration carried by connect commands.
    ##
    ## The default is disabled, so existing publish behavior is unchanged. When
    ## enabled, maxMessages/maxBytes bound the local offline publish queue.
    enabled*: bool
    maxMessages*: int
    maxBytes*: int
    qos0Policy*: MqttOfflineQos0Policy

  MqttCommand* = object
    ## Command sent to the MQTT worker.
    ##
    ## The worker owns the LowLevelClient/libmosquitto handle. Application and
    ## high-level code should send these commands instead of calling
    ## libmosquitto directly.
    id*: int
    kind*: MqttCommandKind
    host*: string
    port*: int
    keepalive*: int
    protocolVersion*: MqttProtocolVersion
    connectProperties*: MqttConnectProperties
    reconnectPolicy*: MqttReconnectPolicy
    offlineQueuePolicy*: MqttOfflineQueuePolicy
    username*: string
    password*: string
    tls*: MqttTlsConfig
    will*: MqttWill
    topic*: string
    payload*: seq[byte]
    qos*: MqttQos
    retain*: bool
    properties*: MqttProperties

  MqttPendingOperations* = object
    ## Snapshot of broker-acknowledged operations that are currently in flight.
    ##
    ## These counters are emitted by the worker when its pending mid tables
    ## change.  They do not include commands that have merely been accepted into
    ## the local command queue but have not been processed by the worker yet.
    publishes*: int
    subscribes*: int
    unsubscribes*: int
    total*: int

  MqttQueueSnapshot* = object
    ## Snapshot of worker-visible MQTT queues.
    ##
    ## pending is the broker-level in-flight snapshot. offlineQueued/offlineBytes
    ## represent publish commands retained locally while the client is offline or
    ## reconnecting. The total is intended for compatibility/debug helpers such as
    ## msgQueue().
    pending*: MqttPendingOperations
    offlineQueued*: int
    offlineBytes*: int
    total*: int

  MqttEvent* = object
    ## Event emitted by the MQTT worker.
    ##
    ## Errors are modelled as normal events because MQTT disconnects, broker
    ## reason codes, and failed commands are part of the client's state machine.
    commandId*: int
    kind*: MqttEventKind
    state*: MqttConnectionState
    mid*: int
    message*: MqttMessage
    error*: MqttError
    detail*: string
    reasonCode*: int
    flags*: int
    grantedQos*: seq[int]
    properties*: MqttProperties
    pending*: MqttPendingOperations
    queue*: MqttQueueSnapshot
    reconnectDelayMs*: int
    reconnectAttempt*: int


# ------------------------------------------------------------------------------
# Connection state helpers
# ------------------------------------------------------------------------------
proc isConnected*(state: MqttConnectionState): bool {.inline.} =
  result = state == mcsConnected

proc isConnecting*(state: MqttConnectionState): bool {.inline.} =
  result = state in {mcsConnecting, mcsReconnecting}

proc isTerminal*(state: MqttConnectionState): bool {.inline.} =
  result = state in {mcsStopped, mcsError}

# ------------------------------------------------------------------------------
# Reconnect policy helpers
# ------------------------------------------------------------------------------
proc noReconnect*(): MqttReconnectPolicy =
  result = MqttReconnectPolicy(
    enabled: false,
    initialDelayMs: 0,
    maxDelayMs: 0,
    multiplier: 1.0
  )

proc mqttReconnectPolicy*(initialDelayMs = 1000; maxDelayMs = 30000;
                          multiplier = 2.0): MqttReconnectPolicy =
  ## Construct an enabled reconnect policy.
  ##
  ## Validation is performed by validateReconnectPolicy() and by the highlevel /
  ## nmqtt setter APIs. Keeping this constructor allocation-only allows tests to
  ## build invalid values deliberately and verify validation paths.
  result = MqttReconnectPolicy(
    enabled: true,
    initialDelayMs: initialDelayMs,
    maxDelayMs: maxDelayMs,
    multiplier: multiplier
  )

proc validateReconnectPolicy*(policy: MqttReconnectPolicy;
                              context = "MQTT reconnect policy"): MqttResult[MqttOk] =
  ## Validate reconnect policy values.
  ##
  ## Disabled policies are accepted regardless of delay fields because the worker
  ## ignores them. Enabled policies must have a non-negative initial delay, a max
  ## delay greater than or equal to the initial delay, and a multiplier of at least
  ## 1.0.
  if not policy.enabled:
    return ok(MqttOk())

  if policy.initialDelayMs < 0:
    return err(invalidArgument(context, &"initialDelayMs must not be negative: {policy.initialDelayMs}"))
  if policy.maxDelayMs < policy.initialDelayMs:
    return err(invalidArgument(context, &"maxDelayMs must be >= initialDelayMs: {policy.maxDelayMs} < {policy.initialDelayMs}"))
  if policy.multiplier < 1.0:
    return err(invalidArgument(context, &"multiplier must be >= 1.0: {policy.multiplier}"))

  result = ok(MqttOk())


proc reconnectDelayMs*(policy: MqttReconnectPolicy; attempt: int): int =
  ## Return the exponential-backoff delay for a 1-based reconnect attempt.
  ##
  ## The policy is assumed to be validated. Invalid/disabled inputs are handled
  ## defensively so this helper is safe to use in tests and diagnostics.
  if not policy.enabled or attempt <= 0:
    return 0

  var delay = policy.initialDelayMs.float
  if attempt > 1:
    for _ in 2 .. attempt:
      delay = delay * policy.multiplier
      if delay >= policy.maxDelayMs.float:
        delay = policy.maxDelayMs.float
        break

  if delay < 0.0:
    return 0
  if delay > policy.maxDelayMs.float:
    delay = policy.maxDelayMs.float

  result = delay.int

proc `$`*(policy: MqttReconnectPolicy): string =
  if policy.enabled:
    result = &"reconnect(enabled=true, initialDelayMs={policy.initialDelayMs}, maxDelayMs={policy.maxDelayMs}, multiplier={policy.multiplier})"
  else:
    result = "reconnect(enabled=false)"


# ------------------------------------------------------------------------------
# Offline queue policy helpers
# ------------------------------------------------------------------------------
proc noOfflineQueue*(): MqttOfflineQueuePolicy =
  result = MqttOfflineQueuePolicy(
    enabled: false,
    maxMessages: 0,
    maxBytes: 0,
    qos0Policy: moqReject
  )

proc mqttOfflineQueuePolicy*(maxMessages = 100; maxBytes = 1024 * 1024;
                             qos0Policy = moqQueue): MqttOfflineQueuePolicy =
  ## Construct an enabled offline publish queue policy.
  ##
  ## The default queues QoS0 publishes as well, matching the original nmqtt
  ## workQueue-style behaviour. Validation is performed by
  ## validateOfflineQueuePolicy(). Keeping the constructor allocation-only lets
  ## tests deliberately build invalid values.
  result = MqttOfflineQueuePolicy(
    enabled: true,
    maxMessages: maxMessages,
    maxBytes: maxBytes,
    qos0Policy: qos0Policy
  )

proc validateOfflineQueuePolicy*(policy: MqttOfflineQueuePolicy;
                                 context = "MQTT offline queue policy"): MqttResult[MqttOk] =
  ## Validate offline publish queue policy values.
  ##
  ## Disabled policies are accepted regardless of limit fields. Enabled policies
  ## must be bounded so disconnected publish queueing cannot become unbounded.
  if not policy.enabled:
    return ok(MqttOk())

  if policy.maxMessages <= 0:
    return err(invalidArgument(context, &"maxMessages must be positive: {policy.maxMessages}"))
  if policy.maxBytes <= 0:
    return err(invalidArgument(context, &"maxBytes must be positive: {policy.maxBytes}"))

  result = ok(MqttOk())

proc `$`*(policy: MqttOfflineQueuePolicy): string =
  if policy.enabled:
    result = &"offlineQueue(enabled=true, maxMessages={policy.maxMessages}, maxBytes={policy.maxBytes}, qos0Policy={policy.qos0Policy})"
  else:
    result = "offlineQueue(enabled=false)"

# ------------------------------------------------------------------------------
# Pending operation helpers
# ------------------------------------------------------------------------------
proc pendingOperations*(publishes = 0; subscribes = 0; unsubscribes = 0): MqttPendingOperations =
  result = MqttPendingOperations(
    publishes: publishes,
    subscribes: subscribes,
    unsubscribes: unsubscribes,
    total: publishes + subscribes + unsubscribes
  )

proc emptyPendingOperations*(): MqttPendingOperations {.inline.} =
  result = pendingOperations()

proc isEmpty*(pending: MqttPendingOperations): bool {.inline.} =
  result = pending.total == 0

# ------------------------------------------------------------------------------
# Queue snapshot helpers
# ------------------------------------------------------------------------------
proc queueSnapshot*(pending: MqttPendingOperations = emptyPendingOperations();
                    offlineQueued = 0; offlineBytes = 0): MqttQueueSnapshot =
  result = MqttQueueSnapshot(
    pending: pending,
    offlineQueued: offlineQueued,
    offlineBytes: offlineBytes,
    total: pending.total + offlineQueued
  )

proc emptyQueueSnapshot*(): MqttQueueSnapshot {.inline.} =
  result = queueSnapshot()

proc isEmpty*(queue: MqttQueueSnapshot): bool {.inline.} =
  result = queue.total == 0 and queue.offlineBytes == 0

# ------------------------------------------------------------------------------
# Payload helpers
# ------------------------------------------------------------------------------
proc payloadString*(command: MqttCommand): string =
  ## Return command payload bytes as a Nim string.
  ##
  ## This mirrors lowlevel_types.payloadString(MqttMessage) and is intended for
  ## tests/debug output. Binary payload users should use command.payload.
  if command.payload.len == 0:
    return ""

  result = newString(command.payload.len)
  copyMem(addr result[0], unsafeAddr command.payload[0], command.payload.len)

# ------------------------------------------------------------------------------
# Command constructors
# ------------------------------------------------------------------------------
proc connectCommand*(host: string; port = 1883; keepalive = 60;
                     protocolVersion = mpv311;
                     reconnectPolicy: MqttReconnectPolicy = MqttReconnectPolicy(enabled: false, multiplier: 1.0);
                     offlineQueuePolicy: MqttOfflineQueuePolicy = MqttOfflineQueuePolicy(enabled: false, qos0Policy: moqReject);
                     username = ""; password = "";
                     tls: MqttTlsConfig = MqttTlsConfig(enabled: false);
                     will: MqttWill = MqttWill(enabled: false, qos: qos0);
                     connectProperties: MqttConnectProperties = MqttConnectProperties();
                     id = 0): MqttCommand =
  result = MqttCommand(
    id: id,
    kind: mckConnect,
    host: host,
    port: port,
    keepalive: keepalive,
    protocolVersion: protocolVersion,
    connectProperties: connectProperties,
    reconnectPolicy: reconnectPolicy,
    offlineQueuePolicy: offlineQueuePolicy,
    username: username,
    password: password,
    tls: tls,
    will: will,
    qos: qos0
  )

proc disconnectCommand*(id = 0): MqttCommand =
  result = MqttCommand(id: id, kind: mckDisconnect, qos: qos0)

proc stopCommand*(id = 0): MqttCommand =
  result = MqttCommand(id: id, kind: mckStop, qos: qos0)

proc publishCommand*(topic: string; payload: openArray[byte]; qos = qos0;
                     retain = false; id = 0;
                     properties: MqttProperties = @[]): MqttCommand =
  result = MqttCommand(
    id: id,
    kind: mckPublish,
    topic: topic,
    payload: @payload,
    qos: qos,
    retain: retain,
    properties: properties
  )

proc publishCommand*(topic: string; payload: string; qos = qos0;
                     retain = false; id = 0;
                     properties: MqttProperties = @[]): MqttCommand =
  result = publishCommand(topic, bytesFromString(payload), qos, retain, id, properties)

proc publishV5Command*(topic: string; payload: openArray[byte]; qos = qos0;
                       retain = false; id = 0;
                       properties: MqttProperties = @[]): MqttCommand =
  ## Construct a PUBLISH command carrying MQTT v5 properties.
  ##
  ## The worker still decides whether to call mosquitto_publish() or
  ## mosquitto_publish_v5() based on whether properties are present.
  result = publishCommand(topic, payload, qos = qos, retain = retain, id = id, properties = properties)

proc publishV5Command*(topic: string; payload: string; qos = qos0;
                       retain = false; id = 0;
                       properties: MqttProperties = @[]): MqttCommand =
  result = publishV5Command(topic, bytesFromString(payload), qos = qos, retain = retain, id = id, properties = properties)

proc publishV5Command*(topic: string; payload: openArray[byte];
                       properties: MqttPublishProperties;
                       qos = qos0; retain = false; id = 0): MqttCommand =
  ## Construct a PUBLISH command carrying typed MQTT v5 PUBLISH properties.
  result = publishV5Command(topic, payload, qos = qos, retain = retain, id = id, properties = properties.toMqttProperties())

proc publishV5Command*(topic: string; payload: string;
                       properties: MqttPublishProperties;
                       qos = qos0; retain = false; id = 0): MqttCommand =
  result = publishV5Command(topic, bytesFromString(payload), qos = qos, retain = retain, id = id, properties = properties)

proc subscribeCommand*(topicFilter: string; qos = qos0; id = 0;
                       properties: MqttProperties = @[]): MqttCommand =
  result = MqttCommand(
    id: id,
    kind: mckSubscribe,
    topic: topicFilter,
    qos: qos,
    properties: properties
  )

proc subscribeV5Command*(topicFilter: string; qos = qos0; id = 0;
                         properties: MqttProperties = @[]): MqttCommand =
  ## Construct a SUBSCRIBE command carrying MQTT v5 SUBSCRIBE properties.
  result = subscribeCommand(topicFilter, qos = qos, id = id, properties = properties)

proc subscribeV5Command*(topicFilter: string; properties: MqttSubscribeProperties;
                         qos = qos0; id = 0): MqttCommand =
  ## Construct a SUBSCRIBE command carrying typed MQTT v5 SUBSCRIBE properties.
  result = subscribeV5Command(topicFilter, qos = qos, id = id, properties = properties.toMqttProperties())

proc unsubscribeCommand*(topicFilter: string; id = 0): MqttCommand =
  result = MqttCommand(
    id: id,
    kind: mckUnsubscribe,
    topic: topicFilter,
    qos: qos0
  )

# ------------------------------------------------------------------------------
# Event constructors
# ------------------------------------------------------------------------------
proc stateChangedEvent*(state: MqttConnectionState; commandId = 0; detail = ""): MqttEvent =
  result = MqttEvent(
    commandId: commandId,
    kind: mevStateChanged,
    state: state,
    detail: detail
  )

proc pendingChangedEvent*(pending: MqttPendingOperations; commandId = 0): MqttEvent =
  result = MqttEvent(
    commandId: commandId,
    kind: mevPendingChanged,
    pending: pending,
    queue: queueSnapshot(pending = pending)
  )

proc queueChangedEvent*(queue: MqttQueueSnapshot; commandId = 0): MqttEvent =
  result = MqttEvent(
    commandId: commandId,
    kind: mevQueueChanged,
    pending: queue.pending,
    queue: queue
  )

proc connectedEvent*(commandId = 0; reasonCode = 0; flags = 0;
                     properties: MqttProperties = @[]): MqttEvent =
  result = MqttEvent(
    commandId: commandId,
    kind: mevConnected,
    state: mcsConnected,
    reasonCode: reasonCode,
    flags: flags,
    properties: properties
  )

proc disconnectedEvent*(commandId = 0; detail = ""; reasonCode = 0;
                        properties: MqttProperties = @[]): MqttEvent =
  result = MqttEvent(
    commandId: commandId,
    kind: mevDisconnected,
    state: mcsDisconnected,
    detail: detail,
    reasonCode: reasonCode,
    properties: properties
  )

proc publishAcceptedEvent*(mid: int; commandId = 0): MqttEvent =
  ## PUBLISH was accepted by libmosquitto and assigned a message id.
  ##
  ## This is intentionally separate from PublishCompleted, which is emitted by
  ## libmosquitto's on_publish callback after QoS1/2 completion.
  result = MqttEvent(commandId: commandId, kind: mevPublishAccepted, mid: mid)

proc publishCompletedEvent*(mid: int; commandId = 0; reasonCode = 0;
                            properties: MqttProperties = @[]): MqttEvent =
  ## PUBLISH completion callback was received from libmosquitto.
  result = MqttEvent(
    commandId: commandId,
    kind: mevPublishCompleted,
    mid: mid,
    reasonCode: reasonCode,
    properties: properties
  )

proc subscribedEvent*(mid: int; commandId = 0; grantedQos: openArray[int] = [];
                      properties: MqttProperties = @[]): MqttEvent =
  result = MqttEvent(
    commandId: commandId,
    kind: mevSubscribed,
    mid: mid,
    grantedQos: @grantedQos,
    properties: properties
  )

proc unsubscribedEvent*(mid: int; commandId = 0;
                        properties: MqttProperties = @[]): MqttEvent =
  result = MqttEvent(commandId: commandId, kind: mevUnsubscribed, mid: mid, properties: properties)

proc reconnectScheduledEvent*(delayMs: int; attempt: int; commandId = 0;
                              detail = ""): MqttEvent =
  result = MqttEvent(
    commandId: commandId,
    kind: mevReconnectScheduled,
    state: mcsReconnecting,
    reconnectDelayMs: delayMs,
    reconnectAttempt: attempt,
    detail: detail
  )

proc reconnectAttemptEvent*(attempt: int; commandId = 0; detail = ""): MqttEvent =
  result = MqttEvent(
    commandId: commandId,
    kind: mevReconnectAttempt,
    state: mcsReconnecting,
    reconnectAttempt: attempt,
    detail: detail
  )

proc messageReceivedEvent*(message: sink MqttMessage; commandId = 0): MqttEvent =
  result = MqttEvent(
    commandId: commandId,
    kind: mevMessageReceived,
    message: move message
  )

proc errorEvent*(error: MqttError; commandId = 0; reasonCode = 0;
                 properties: MqttProperties = @[]): MqttEvent =
  result = MqttEvent(
    commandId: commandId,
    kind: mevError,
    error: error,
    reasonCode: reasonCode,
    properties: properties
  )

proc stoppedEvent*(commandId = 0): MqttEvent =
  result = MqttEvent(commandId: commandId, kind: mevStopped, state: mcsStopped)

# ------------------------------------------------------------------------------
# Debug formatting
# ------------------------------------------------------------------------------
proc summary*(command: MqttCommand): string =
  case command.kind
  of mckConnect:
    let auth = if command.username.len > 0: ", auth=true" else: ""
    let tls = if command.tls.enabled: ", tls=true" else: ""
    let will = if command.will.enabled: ", will=true" else: ""
    let connectProps = if command.connectProperties.hasProperties(): ", connectProperties=true" else: ""
    let reconnect = if command.reconnectPolicy.enabled: ", reconnect=true" else: ""
    let offlineQueue = if command.offlineQueuePolicy.enabled: ", offlineQueue=true" else: ""
    result = &"{command.kind}(id={command.id}, host={command.host}, port={command.port}, protocol={command.protocolVersion}{auth}{tls}{will}{connectProps}{reconnect}{offlineQueue})"
  of mckPublish:
    let props = if command.properties.len > 0: &", properties={command.properties.len}" else: ""
    result = &"{command.kind}(id={command.id}, topic={command.topic}, payloadLen={command.payload.len}, qos={command.qos}, retain={command.retain}{props})"
  of mckSubscribe, mckUnsubscribe:
    result = &"{command.kind}(id={command.id}, topic={command.topic}, qos={command.qos})"
  of mckDisconnect, mckStop:
    result = &"{command.kind}(id={command.id})"

proc summary*(event: MqttEvent): string =
  case event.kind
  of mevStateChanged:
    result = &"{event.kind}(commandId={event.commandId}, state={event.state}, detail={event.detail})"
  of mevPendingChanged:
    result = &"{event.kind}(commandId={event.commandId}, publishes={event.pending.publishes}, subscribes={event.pending.subscribes}, unsubscribes={event.pending.unsubscribes}, total={event.pending.total})"
  of mevQueueChanged:
    result = &"{event.kind}(commandId={event.commandId}, pending={event.queue.pending.total}, offlineQueued={event.queue.offlineQueued}, offlineBytes={event.queue.offlineBytes}, total={event.queue.total})"
  of mevReconnectScheduled:
    result = &"{event.kind}(commandId={event.commandId}, attempt={event.reconnectAttempt}, delayMs={event.reconnectDelayMs}, detail={event.detail})"
  of mevReconnectAttempt:
    result = &"{event.kind}(commandId={event.commandId}, attempt={event.reconnectAttempt}, detail={event.detail})"
  of mevMessageReceived:
    result = &"{event.kind}(commandId={event.commandId}, topic={event.message.topic}, payloadLen={event.message.payload.len})"
  of mevError:
    let props = if event.properties.len > 0: &", reasonCode={event.reasonCode}, properties={event.properties.len}" else: ""
    result = &"{event.kind}(commandId={event.commandId}, error={event.error}{props})"
  of mevPublishAccepted:
    result = &"{event.kind}(commandId={event.commandId}, mid={event.mid}, reasonCode={event.reasonCode})"
  of mevPublishCompleted, mevUnsubscribed:
    let props = if event.properties.len > 0: &", properties={event.properties.len}" else: ""
    result = &"{event.kind}(commandId={event.commandId}, mid={event.mid}, reasonCode={event.reasonCode}{props})"
  of mevSubscribed:
    let props = if event.properties.len > 0: &", properties={event.properties.len}" else: ""
    result = &"{event.kind}(commandId={event.commandId}, mid={event.mid}, grantedQos={event.grantedQos}{props})"
  of mevDisconnected:
    let props = if event.properties.len > 0: &", properties={event.properties.len}" else: ""
    result = &"{event.kind}(commandId={event.commandId}, state={event.state}, detail={event.detail}, reasonCode={event.reasonCode}{props})"
  of mevConnected:
    let props = if event.properties.len > 0: &", properties={event.properties.len}" else: ""
    result = &"{event.kind}(commandId={event.commandId}, state={event.state}, reasonCode={event.reasonCode}, flags={event.flags}{props})"
  of mevStopped:
    result = &"{event.kind}(commandId={event.commandId})"
