# Destination: src/mosquitto_nim/worker/types.nim

import std/strformat

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

  MqttEventKind* = enum
    mevConnected
    mevDisconnected
    mevPublishAccepted
    mevPublishCompleted
    mevSubscribed
    mevUnsubscribed
    mevMessageReceived
    mevError
    mevStopped

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
    topic*: string
    payload*: seq[byte]
    qos*: MqttQos
    retain*: bool

  MqttEvent* = object
    ## Event emitted by the MQTT worker.
    ##
    ## Errors are modelled as normal events because MQTT disconnects, broker
    ## reason codes, and failed commands are part of the client's state machine.
    commandId*: int
    kind*: MqttEventKind
    mid*: int
    message*: MqttMessage
    error*: MqttError
    detail*: string
    reasonCode*: int
    flags*: int
    grantedQos*: seq[int]

# ------------------------------------------------------------------------------
# Payload helpers
# ------------------------------------------------------------------------------
proc bytesFromString*(payload: string): seq[byte] =
  ## Convert a Nim string to MQTT payload bytes.
  if payload.len == 0:
    return @[]

  result = newSeq[byte](payload.len)
  copyMem(addr result[0], unsafeAddr payload[0], payload.len)

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
                     id = 0): MqttCommand =
  result = MqttCommand(
    id: id,
    kind: mckConnect,
    host: host,
    port: port,
    keepalive: keepalive,
    qos: qos0
  )

proc disconnectCommand*(id = 0): MqttCommand =
  result = MqttCommand(id: id, kind: mckDisconnect, qos: qos0)

proc stopCommand*(id = 0): MqttCommand =
  result = MqttCommand(id: id, kind: mckStop, qos: qos0)

proc publishCommand*(topic: string; payload: openArray[byte]; qos = qos0;
                     retain = false; id = 0): MqttCommand =
  result = MqttCommand(
    id: id,
    kind: mckPublish,
    topic: topic,
    payload: @payload,
    qos: qos,
    retain: retain
  )

proc publishCommand*(topic: string; payload: string; qos = qos0;
                     retain = false; id = 0): MqttCommand =
  result = publishCommand(topic, bytesFromString(payload), qos, retain, id)

proc subscribeCommand*(topicFilter: string; qos = qos0; id = 0): MqttCommand =
  result = MqttCommand(
    id: id,
    kind: mckSubscribe,
    topic: topicFilter,
    qos: qos
  )

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
proc connectedEvent*(commandId = 0; reasonCode = 0; flags = 0): MqttEvent =
  result = MqttEvent(
    commandId: commandId,
    kind: mevConnected,
    reasonCode: reasonCode,
    flags: flags
  )

proc disconnectedEvent*(commandId = 0; detail = ""; reasonCode = 0): MqttEvent =
  result = MqttEvent(
    commandId: commandId,
    kind: mevDisconnected,
    detail: detail,
    reasonCode: reasonCode
  )

proc publishAcceptedEvent*(mid: int; commandId = 0): MqttEvent =
  ## PUBLISH was accepted by libmosquitto and assigned a message id.
  ##
  ## This is intentionally separate from PublishCompleted, which is emitted by
  ## libmosquitto's on_publish callback after QoS1/2 completion.
  result = MqttEvent(commandId: commandId, kind: mevPublishAccepted, mid: mid)

proc publishCompletedEvent*(mid: int; commandId = 0; reasonCode = 0): MqttEvent =
  ## PUBLISH completion callback was received from libmosquitto.
  result = MqttEvent(
    commandId: commandId,
    kind: mevPublishCompleted,
    mid: mid,
    reasonCode: reasonCode
  )

proc subscribedEvent*(mid: int; commandId = 0; grantedQos: openArray[int] = []): MqttEvent =
  result = MqttEvent(
    commandId: commandId,
    kind: mevSubscribed,
    mid: mid,
    grantedQos: @grantedQos
  )

proc unsubscribedEvent*(mid: int; commandId = 0): MqttEvent =
  result = MqttEvent(commandId: commandId, kind: mevUnsubscribed, mid: mid)

proc messageReceivedEvent*(message: sink MqttMessage; commandId = 0): MqttEvent =
  result = MqttEvent(
    commandId: commandId,
    kind: mevMessageReceived,
    message: move message
  )

proc errorEvent*(error: MqttError; commandId = 0): MqttEvent =
  result = MqttEvent(commandId: commandId, kind: mevError, error: error)

proc stoppedEvent*(commandId = 0): MqttEvent =
  result = MqttEvent(commandId: commandId, kind: mevStopped)

# ------------------------------------------------------------------------------
# Debug formatting
# ------------------------------------------------------------------------------
proc summary*(command: MqttCommand): string =
  case command.kind
  of mckConnect:
    result = &"{command.kind}(id={command.id}, host={command.host}, port={command.port})"
  of mckPublish:
    result = &"{command.kind}(id={command.id}, topic={command.topic}, payloadLen={command.payload.len}, qos={command.qos}, retain={command.retain})"
  of mckSubscribe, mckUnsubscribe:
    result = &"{command.kind}(id={command.id}, topic={command.topic}, qos={command.qos})"
  of mckDisconnect, mckStop:
    result = &"{command.kind}(id={command.id})"

proc summary*(event: MqttEvent): string =
  case event.kind
  of mevMessageReceived:
    result = &"{event.kind}(commandId={event.commandId}, topic={event.message.topic}, payloadLen={event.message.payload.len})"
  of mevError:
    result = &"{event.kind}(commandId={event.commandId}, error={event.error})"
  of mevPublishAccepted, mevPublishCompleted, mevUnsubscribed:
    result = &"{event.kind}(commandId={event.commandId}, mid={event.mid}, reasonCode={event.reasonCode})"
  of mevSubscribed:
    result = &"{event.kind}(commandId={event.commandId}, mid={event.mid}, grantedQos={event.grantedQos})"
  of mevDisconnected:
    result = &"{event.kind}(commandId={event.commandId}, detail={event.detail}, reasonCode={event.reasonCode})"
  of mevConnected:
    result = &"{event.kind}(commandId={event.commandId}, reasonCode={event.reasonCode}, flags={event.flags})"
  of mevStopped:
    result = &"{event.kind}(commandId={event.commandId})"
