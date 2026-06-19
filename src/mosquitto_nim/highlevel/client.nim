# Destination: src/mosquitto_nim/highlevel/client.nim

import std/asyncdispatch

import results

import ../lowlevel/errors
import ../lowlevel/types
import ../worker/types
import ../worker/mosquitto_worker
import ./async_bridge

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
  MqttClient* = ref object
    worker: MqttWorker
    bridge: MqttAsyncBridge
    nextCommandId: int
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
  client.nextCommandId = 0
  client.closed = false
  result = ok(client)

proc isClosed*(client: MqttClient): bool {.inline.} =
  result = client.isNil or client.closed

proc isStarted*(client: MqttClient): bool {.inline.} =
  result = not client.isNil and not client.closed and
           not client.worker.isNil and client.worker.isStarted

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
  result = ok(MqttOk())

proc mqttWorker*(client: MqttClient): MqttWorker =
  ## Return the owned worker for tests/diagnostics.
  ##
  ## Application code should normally use highlevel methods instead of touching
  ## the worker directly.
  if client.isNil:
    return nil
  result = client.worker

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
              keepalive = 60): MqttResult[int] =
  var cmd = connectCommand(host, port = port, keepalive = keepalive)
  result = client.sendClientCommand(move cmd, "connect MQTT client")

proc disconnect*(client: MqttClient): MqttResult[int] =
  var cmd = disconnectCommand()
  result = client.sendClientCommand(move cmd, "disconnect MQTT client")

proc requestStop*(client: MqttClient): MqttResult[int] =
  var cmd = stopCommand()
  result = client.sendClientCommand(move cmd, "stop MQTT client")

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

proc subscribe*(client: MqttClient; topicFilter: string; qos = qos0): MqttResult[int] =
  var cmd = subscribeCommand(topicFilter, qos = qos)
  result = client.sendClientCommand(move cmd, "subscribe MQTT topic")

proc unsubscribe*(client: MqttClient; topicFilter: string): MqttResult[int] =
  var cmd = unsubscribeCommand(topicFilter)
  result = client.sendClientCommand(move cmd, "unsubscribe MQTT topic")

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

  return await client.bridge.nextEvent()

proc drainEvents*(client: MqttClient): MqttResult[seq[MqttEvent]] =
  ## Drain currently queued worker events without waiting.
  if client.isNil:
    return err(invalidState("MQTT client drainEvents", "client is nil"))
  if client.closed:
    return err(invalidState("MQTT client drainEvents", "client is closed"))
  if client.bridge.isNil:
    return err(invalidState("MQTT client drainEvents", "async bridge is nil"))

  result = client.bridge.drainEvents()
