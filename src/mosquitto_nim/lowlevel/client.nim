# Destination: src/mosquitto_nim/lowlevel/client.nim

import std/options

import results

import ./bindings/c_api
import ./bridge
import ./errors
import ./types

# ------------------------------------------------------------------------------
# Low-level client handle wrapper.
#
# This layer owns a libmosquitto client handle and provides thin wrappers around
# the synchronous/manual-loop C API. It intentionally keeps raw pointers private
# and does not start libmosquitto's internal worker thread.
# ------------------------------------------------------------------------------
type
  LowLevelClient* = ref object
    raw: ptr struct_mosquitto
    messageSink: MessageSink
    callbackError: Option[MqttError]

proc isClosed*(client: LowLevelClient): bool =
  result = client.isNil or client.raw == nil

proc requireOpen(client: LowLevelClient; context: string): MqttResult[ptr struct_mosquitto] =
  if client.isClosed:
    return err(invalidState(context, "low-level client handle is closed"))

  result = ok(client.raw)

proc rememberCallbackError(client: LowLevelClient; error: MqttError) =
  if not client.isNil:
    client.callbackError = some(error)

proc onLowLevelMessage(mosq: ptr struct_mosquitto; userdata: pointer;
                       message: ptr struct_mosquitto_message;
                       properties: ptr mosquitto_property) {.cdecl.} =
  ## libmosquitto message callback trampoline.
  ##
  ## This callback must not call application handlers directly. It only copies
  ## C-owned callback data into a Nim-owned MqttMessage and forwards that object
  ## to the low-level sink. Higher layers will use the sink to enqueue events.
  discard mosq

  if userdata == nil:
    return

  let client = cast[LowLevelClient](userdata)
  if client.isNil or client.isClosed:
    return

  let msgRes = copyMessage(message, properties)
  if msgRes.isErr:
    client.rememberCallbackError(msgRes.error)
    return

  if client.messageSink.isNil:
    return

  try:
    client.messageSink(msgRes.get())
  except CatchableError as e:
    client.rememberCallbackError(invalidState("message callback", e.msg))

proc newLowLevelClient*(clientId: string; cleanSession = true): MqttResult[LowLevelClient] =
  ## Create a libmosquitto client handle.
  ##
  ## An empty clientId is passed as nil, allowing libmosquitto to generate a
  ## client id where supported. The raw handle is intentionally kept private.
  var cClientId: cstring = nil
  if clientId.len > 0:
    cClientId = clientId.cstring

  let raw = mosquitto_new(cClientId, cleanSession, nil)
  if raw == nil:
    return err(makeError(
      meLibraryError,
      "mosquitto_new",
      "failed to create libmosquitto client handle"
    ))

  let client = LowLevelClient(raw: raw)
  mosquitto_user_data_set(raw, cast[pointer](client))
  mosquitto_message_v5_callback_set(raw, onLowLevelMessage)

  result = ok(client)

proc closeLowLevelClient*(client: LowLevelClient): MqttResult[MqttOk] =
  ## Destroy the libmosquitto client handle.
  ##
  ## This operation is intentionally idempotent so cleanup paths can call it
  ## safely after partial initialization failures.
  if client.isNil or client.raw == nil:
    return ok(MqttOk())

  mosquitto_user_data_set(client.raw, nil)
  mosquitto_destroy(client.raw)
  client.raw = nil
  client.messageSink = nil
  result = ok(MqttOk())

# ------------------------------------------------------------------------------
# Low-level callback sink helpers.
# ------------------------------------------------------------------------------
proc setMessageSink*(client: LowLevelClient; sink: MessageSink): MqttResult[MqttOk] =
  ## Install a low-level message sink.
  ##
  ## The sink is called from the thread that drives `loopLowLevelClient()`. It is
  ## intended for worker/event-queue plumbing, not for direct application logic.
  let rawRes = requireOpen(client, "set message sink")
  if rawRes.isErr:
    return err(rawRes.error)

  client.messageSink = sink
  result = ok(MqttOk())

proc clearMessageSink*(client: LowLevelClient): MqttResult[MqttOk] =
  let rawRes = requireOpen(client, "clear message sink")
  if rawRes.isErr:
    return err(rawRes.error)

  client.messageSink = nil
  result = ok(MqttOk())

proc lastCallbackError*(client: LowLevelClient): Option[MqttError] =
  if client.isNil:
    return none(MqttError)

  result = client.callbackError

proc clearCallbackError*(client: LowLevelClient): MqttResult[MqttOk] =
  let rawRes = requireOpen(client, "clear callback error")
  if rawRes.isErr:
    return err(rawRes.error)

  client.callbackError = none(MqttError)
  result = ok(MqttOk())

# ------------------------------------------------------------------------------
# Topic validation helpers.
# ------------------------------------------------------------------------------
proc validatePublishTopic*(topic: string): MqttResult[MqttOk] =
  ## Validate a topic name for PUBLISH.
  ##
  ## This rejects wildcard-containing topic filters such as "foo/+".
  if topic.len == 0:
    return err(invalidArgument("publish topic", "topic must not be empty"))

  result = checkMosq(mosquitto_pub_topic_check(topic.cstring), "mosquitto_pub_topic_check")

proc validateSubscribeTopic*(topicFilter: string): MqttResult[MqttOk] =
  ## Validate a topic filter for SUBSCRIBE.
  ##
  ## Wildcards are accepted when libmosquitto accepts them.
  if topicFilter.len == 0:
    return err(invalidArgument("subscribe topic", "topic filter must not be empty"))

  result = checkMosq(mosquitto_sub_topic_check(topicFilter.cstring), "mosquitto_sub_topic_check")

# ------------------------------------------------------------------------------
# Manual-loop network wrappers.
# ------------------------------------------------------------------------------
proc connectLowLevelClient*(client: LowLevelClient; host: string;
                            port = 1883; keepalive = 60): MqttResult[MqttOk] =
  ## Start a blocking libmosquitto connection attempt.
  ##
  ## The caller must drive `loopLowLevelClient()` afterwards to process CONNACK,
  ## keepalive, callbacks, and queued packets.
  let rawRes = requireOpen(client, "mosquitto_connect")
  if rawRes.isErr:
    return err(rawRes.error)

  if host.len == 0:
    return err(invalidArgument("mosquitto_connect", "host must not be empty"))
  if port < 1 or port > 65535:
    return err(invalidArgument("mosquitto_connect", "port must be in range 1..65535"))
  if keepalive < 0:
    return err(invalidArgument("mosquitto_connect", "keepalive must not be negative"))

  result = checkMosq(
    mosquitto_connect(rawRes.get(), host.cstring, port.cint, keepalive.cint),
    "mosquitto_connect"
  )

proc disconnectLowLevelClient*(client: LowLevelClient): MqttResult[MqttOk] =
  ## Queue a clean disconnect packet.
  ##
  ## The caller may run the loop a few more times after this call if it wants to
  ## flush the packet before closing the handle.
  let rawRes = requireOpen(client, "mosquitto_disconnect")
  if rawRes.isErr:
    return err(rawRes.error)

  result = checkMosq(mosquitto_disconnect(rawRes.get()), "mosquitto_disconnect")

proc loopLowLevelClient*(client: LowLevelClient; timeoutMs = 50;
                         maxPackets = 1): MqttResult[MqttOk] =
  ## Drive libmosquitto's network loop once.
  ##
  ## Higher layers will call this from a dedicated Nim-managed worker thread.
  let rawRes = requireOpen(client, "mosquitto_loop")
  if rawRes.isErr:
    return err(rawRes.error)

  if timeoutMs < 0:
    return err(invalidArgument("mosquitto_loop", "timeoutMs must not be negative"))
  if maxPackets < 1:
    return err(invalidArgument("mosquitto_loop", "maxPackets must be at least 1"))

  result = checkMosq(
    mosquitto_loop(rawRes.get(), timeoutMs.cint, maxPackets.cint),
    "mosquitto_loop"
  )

proc publishLowLevelClient*(client: LowLevelClient; topic: string;
                            payload: openArray[byte]; qos = qos0;
                            retain = false): MqttResult[int] =
  ## Queue a PUBLISH packet and return the libmosquitto message id.
  let rawRes = requireOpen(client, "mosquitto_publish")
  if rawRes.isErr:
    return err(rawRes.error)

  let topicRes = validatePublishTopic(topic)
  if topicRes.isErr:
    return err(topicRes.error)

  var mid: cint
  var payloadPtr: pointer = nil
  if payload.len > 0:
    payloadPtr = cast[pointer](unsafeAddr payload[0])

  let rc = mosquitto_publish(
    rawRes.get(),
    addr mid,
    topic.cstring,
    payload.len.cint,
    payloadPtr,
    qos.toInt().cint,
    retain
  )
  let rcRes = checkMosq(rc, "mosquitto_publish")
  if rcRes.isErr:
    return err(rcRes.error)

  result = ok(mid.int)

proc publishLowLevelClient*(client: LowLevelClient; topic: string;
                            payload: string; qos = qos0;
                            retain = false): MqttResult[int] =
  ## Queue a text PUBLISH packet and return the libmosquitto message id.
  var bytes = newSeq[byte](payload.len)
  for i, ch in payload:
    bytes[i] = byte(ord(ch))

  result = publishLowLevelClient(client, topic, bytes, qos, retain)

proc subscribeLowLevelClient*(client: LowLevelClient; topicFilter: string;
                              qos = qos0): MqttResult[int] =
  ## Queue a SUBSCRIBE packet and return the libmosquitto message id.
  let rawRes = requireOpen(client, "mosquitto_subscribe")
  if rawRes.isErr:
    return err(rawRes.error)

  let topicRes = validateSubscribeTopic(topicFilter)
  if topicRes.isErr:
    return err(topicRes.error)

  var mid: cint
  let rc = mosquitto_subscribe(
    rawRes.get(),
    addr mid,
    topicFilter.cstring,
    qos.toInt().cint
  )
  let rcRes = checkMosq(rc, "mosquitto_subscribe")
  if rcRes.isErr:
    return err(rcRes.error)

  result = ok(mid.int)

proc unsubscribeLowLevelClient*(client: LowLevelClient; topicFilter: string): MqttResult[int] =
  ## Queue an UNSUBSCRIBE packet and return the libmosquitto message id.
  let rawRes = requireOpen(client, "mosquitto_unsubscribe")
  if rawRes.isErr:
    return err(rawRes.error)

  let topicRes = validateSubscribeTopic(topicFilter)
  if topicRes.isErr:
    return err(topicRes.error)

  var mid: cint
  let rc = mosquitto_unsubscribe(rawRes.get(), addr mid, topicFilter.cstring)
  let rcRes = checkMosq(rc, "mosquitto_unsubscribe")
  if rcRes.isErr:
    return err(rcRes.error)

  result = ok(mid.int)
