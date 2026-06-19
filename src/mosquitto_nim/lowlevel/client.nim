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
    controlSink: ControlSink
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

proc emitControlEvent(client: LowLevelClient; event: sink LowLevelControlEvent) =
  if client.isNil or client.isClosed or client.controlSink.isNil:
    return

  try:
    client.controlSink(event)
  except CatchableError as e:
    client.rememberCallbackError(invalidState("control callback", e.msg))

proc clientFromUserdata(userdata: pointer): LowLevelClient =
  if userdata == nil:
    return nil

  result = cast[LowLevelClient](userdata)
  if result.isNil or result.isClosed:
    return nil

proc onLowLevelConnect(mosq: ptr struct_mosquitto; userdata: pointer;
                       reasonCode: cint; flags: cint;
                       properties: ptr mosquitto_property) {.cdecl.} =
  discard mosq
  discard properties

  let client = clientFromUserdata(userdata)
  if client.isNil:
    return

  client.emitControlEvent(LowLevelControlEvent(
    kind: lleConnected,
    reasonCode: reasonCode.int,
    flags: flags.int
  ))

proc onLowLevelDisconnect(mosq: ptr struct_mosquitto; userdata: pointer;
                          reasonCode: cint;
                          properties: ptr mosquitto_property) {.cdecl.} =
  discard mosq
  discard properties

  let client = clientFromUserdata(userdata)
  if client.isNil:
    return

  client.emitControlEvent(LowLevelControlEvent(
    kind: lleDisconnected,
    reasonCode: reasonCode.int
  ))

proc onLowLevelPublish(mosq: ptr struct_mosquitto; userdata: pointer;
                       mid: cint; reasonCode: cint;
                       properties: ptr mosquitto_property) {.cdecl.} =
  discard mosq
  discard properties

  let client = clientFromUserdata(userdata)
  if client.isNil:
    return

  client.emitControlEvent(LowLevelControlEvent(
    kind: llePublishCompleted,
    mid: mid.int,
    reasonCode: reasonCode.int
  ))

proc onLowLevelSubscribe(mosq: ptr struct_mosquitto; userdata: pointer;
                         mid: cint; qosCount: cint; grantedQos: ptr cint;
                         properties: ptr mosquitto_property) {.cdecl.} =
  discard mosq
  discard properties

  let client = clientFromUserdata(userdata)
  if client.isNil:
    return

  var granted: seq[int] = @[]
  if qosCount > 0 and grantedQos != nil:
    let rawGranted = cast[ptr UncheckedArray[cint]](grantedQos)
    granted = newSeq[int](qosCount.int)
    for i in 0 ..< qosCount.int:
      granted[i] = rawGranted[i].int

  client.emitControlEvent(LowLevelControlEvent(
    kind: lleSubscribed,
    mid: mid.int,
    grantedQos: granted
  ))

proc onLowLevelUnsubscribe(mosq: ptr struct_mosquitto; userdata: pointer;
                           mid: cint;
                           properties: ptr mosquitto_property) {.cdecl.} =
  discard mosq
  discard properties

  let client = clientFromUserdata(userdata)
  if client.isNil:
    return

  client.emitControlEvent(LowLevelControlEvent(
    kind: lleUnsubscribed,
    mid: mid.int
  ))

proc onLowLevelMessage(mosq: ptr struct_mosquitto; userdata: pointer;
                       message: ptr struct_mosquitto_message;
                       properties: ptr mosquitto_property) {.cdecl.} =
  ## libmosquitto message callback trampoline.
  ##
  ## This callback must not call application handlers directly. It only copies
  ## C-owned callback data into a Nim-owned MqttMessage and forwards that object
  ## to the low-level sink. Higher layers will use the sink to enqueue events.
  discard mosq

  let client = clientFromUserdata(userdata)
  if client.isNil:
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
  mosquitto_connect_v5_callback_set(raw, onLowLevelConnect)
  mosquitto_disconnect_v5_callback_set(raw, onLowLevelDisconnect)
  mosquitto_publish_v5_callback_set(raw, onLowLevelPublish)
  mosquitto_subscribe_v5_callback_set(raw, onLowLevelSubscribe)
  mosquitto_unsubscribe_v5_callback_set(raw, onLowLevelUnsubscribe)
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
  client.controlSink = nil
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

proc setControlSink*(client: LowLevelClient; sink: ControlSink): MqttResult[MqttOk] =
  ## Install a low-level control/ack sink.
  ##
  ## The sink receives callback notifications such as CONNACK, PUBACK/PUBCOMP,
  ## SUBACK, and UNSUBACK from the thread driving `loopLowLevelClient()`.
  let rawRes = requireOpen(client, "set control sink")
  if rawRes.isErr:
    return err(rawRes.error)

  client.controlSink = sink
  result = ok(MqttOk())

proc clearControlSink*(client: LowLevelClient): MqttResult[MqttOk] =
  let rawRes = requireOpen(client, "clear control sink")
  if rawRes.isErr:
    return err(rawRes.error)

  client.controlSink = nil
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



proc setProtocolVersion*(client: LowLevelClient;
                         protocolVersion = mpv311): MqttResult[MqttOk] =
  ## Configure the MQTT protocol version on the libmosquitto handle.
  ##
  ## This must be called before connectLowLevelClient().  The default for
  ## mosquitto_nim remains MQTT 3.1.1 for nmqtt compatibility, while callers can
  ## explicitly select MQTT 5 when the broker requires it.
  let rawRes = requireOpen(client, "set MQTT protocol version")
  if rawRes.isErr:
    return err(rawRes.error)

  result = checkMosq(
    mosquitto_int_option(
      rawRes.get(),
      MOSQ_OPT_PROTOCOL_VERSION,
      protocolVersion.toInt().cint
    ),
    "mosquitto_int_option(MOSQ_OPT_PROTOCOL_VERSION)"
  )

proc optionalCString(value: string): cstring =
  ## Convert an optional Nim string to the nil-or-cstring style used by
  ## libmosquitto configuration APIs.
  if value.len == 0:
    return nil
  result = value.cstring

proc setTls*(client: LowLevelClient; config: MqttTlsConfig): MqttResult[MqttOk] =
  ## Configure certificate based TLS on the libmosquitto handle.
  ##
  ## This must be called before connectLowLevelClient(). Passing noTls() is a
  ## no-op so higher layers can unconditionally apply optional TLS settings.
  if not config.enabled:
    return ok(MqttOk())

  let rawRes = requireOpen(client, "set MQTT TLS")
  if rawRes.isErr:
    return err(rawRes.error)

  let tlsRes = checkMosq(
    mosquitto_tls_set(
      rawRes.get(),
      optionalCString(config.cafile),
      optionalCString(config.capath),
      optionalCString(config.certfile),
      optionalCString(config.keyfile),
      nil
    ),
    "mosquitto_tls_set"
  )
  if tlsRes.isErr:
    return err(tlsRes.error)

  if config.insecure:
    let insecureRes = checkMosq(
      mosquitto_tls_insecure_set(rawRes.get(), true),
      "mosquitto_tls_insecure_set"
    )
    if insecureRes.isErr:
      return err(insecureRes.error)

  result = ok(MqttOk())

proc setUsernamePassword*(client: LowLevelClient; username: string;
                          password = ""): MqttResult[MqttOk] =
  ## Configure username/password authentication on the libmosquitto handle.
  ##
  ## This must be called before connectLowLevelClient().  Passing an empty
  ## username and empty password clears the configured credentials.  A password
  ## without a username is rejected because libmosquitto requires the username
  ## field to carry password authentication.
  let rawRes = requireOpen(client, "set MQTT username/password")
  if rawRes.isErr:
    return err(rawRes.error)

  if username.len == 0 and password.len > 0:
    return err(invalidArgument(
      "set MQTT username/password",
      "password cannot be set without username"
    ))

  var userC: cstring = nil
  var passC: cstring = nil
  if username.len > 0:
    userC = username.cstring
  if password.len > 0:
    passC = password.cstring

  result = checkMosq(
    mosquitto_username_pw_set(rawRes.get(), userC, passC),
    "mosquitto_username_pw_set"
  )

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


proc setWill*(client: LowLevelClient; topic: string; payload: openArray[byte];
              qos = qos0; retain = false): MqttResult[MqttOk] =
  ## Configure the MQTT Will message on the libmosquitto handle.
  ##
  ## This must be called before connectLowLevelClient().  Payload bytes are
  ## copied by libmosquitto, so the openArray does not escape this call.
  let rawRes = requireOpen(client, "set MQTT will")
  if rawRes.isErr:
    return err(rawRes.error)

  let topicRes = validatePublishTopic(topic)
  if topicRes.isErr:
    return err(topicRes.error)

  var payloadPtr: pointer = nil
  if payload.len > 0:
    payloadPtr = cast[pointer](unsafeAddr payload[0])

  result = checkMosq(
    mosquitto_will_set(
      rawRes.get(),
      topic.cstring,
      payload.len.cint,
      payloadPtr,
      qos.toInt().cint,
      retain
    ),
    "mosquitto_will_set"
  )

proc setWill*(client: LowLevelClient; topic: string; payload: string;
              qos = qos0; retain = false): MqttResult[MqttOk] =
  ## Configure a text MQTT Will message on the libmosquitto handle.
  var bytes = newSeq[byte](payload.len)
  if payload.len > 0:
    copyMem(addr bytes[0], unsafeAddr payload[0], payload.len)

  result = setWill(client, topic, bytes, qos = qos, retain = retain)

proc clearWill*(client: LowLevelClient): MqttResult[MqttOk] =
  ## Clear any MQTT Will configured on the libmosquitto handle.
  let rawRes = requireOpen(client, "clear MQTT will")
  if rawRes.isErr:
    return err(rawRes.error)

  result = checkMosq(mosquitto_will_clear(rawRes.get()), "mosquitto_will_clear")

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

proc reconnectLowLevelClient*(client: LowLevelClient): MqttResult[MqttOk] =
  ## Ask libmosquitto to reconnect using the previous connection settings.
  ##
  ## This is used by the worker's explicit auto-reconnect state machine.  The
  ## worker still drives the network manually with loopLowLevelClient(); it does
  ## not use mosquitto_loop_start().
  let rawRes = requireOpen(client, "mosquitto_reconnect")
  if rawRes.isErr:
    return err(rawRes.error)

  result = checkMosq(mosquitto_reconnect(rawRes.get()), "mosquitto_reconnect")

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

proc publishLowLevelClientV5*(client: LowLevelClient; topic: string;
                              payload: openArray[byte]; qos = qos0;
                              retain = false;
                              properties: MqttProperties = @[]): MqttResult[int] =
  ## Queue an MQTT v5 PUBLISH packet with optional properties.
  ##
  ## This is separate from publishLowLevelClient() so existing MQTT 3.1.1-style
  ## callers keep using mosquitto_publish().  Callers that select MQTT v5 can use
  ## this when they need User Properties or other v5 metadata.
  let rawRes = requireOpen(client, "mosquitto_publish_v5")
  if rawRes.isErr:
    return err(rawRes.error)

  let topicRes = validatePublishTopic(topic)
  if topicRes.isErr:
    return err(topicRes.error)

  let propsRes = buildMosquittoProperties(properties, "publish MQTT v5 properties")
  if propsRes.isErr:
    return err(propsRes.error)

  var rawProps = propsRes.get()
  var mid: cint
  var payloadPtr: pointer = nil
  if payload.len > 0:
    payloadPtr = cast[pointer](unsafeAddr payload[0])

  let rc = mosquitto_publish_v5(
    rawRes.get(),
    addr mid,
    topic.cstring,
    payload.len.cint,
    payloadPtr,
    qos.toInt().cint,
    retain,
    rawProps
  )
  freeMosquittoProperties(rawProps)

  let rcRes = checkMosq(rc, "mosquitto_publish_v5")
  if rcRes.isErr:
    return err(rcRes.error)

  result = ok(mid.int)

proc publishLowLevelClientV5*(client: LowLevelClient; topic: string;
                              payload: string; qos = qos0;
                              retain = false;
                              properties: MqttProperties = @[]): MqttResult[int] =
  ## Queue a text MQTT v5 PUBLISH packet with optional properties.
  var bytes = newSeq[byte](payload.len)
  for i, ch in payload:
    bytes[i] = byte(ord(ch))

  result = publishLowLevelClientV5(client, topic, bytes, qos, retain, properties)

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
  let rc = mosquitto_subscribe(rawRes.get(), addr mid, topicFilter.cstring, qos.toInt().cint)
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
