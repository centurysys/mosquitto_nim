# Destination: src/mosquitto_nim/highlevel/nmqtt_compat.nim

import std/[asyncdispatch, options]

import results

import ../lowlevel/errors
import ../lowlevel/types
import ../worker/types
import ./client as highlevel_client
import ./dispatcher

# ------------------------------------------------------------------------------
# nmqtt-compatible facade.
#
# This module intentionally keeps the old nmqtt-style surface API small and
# callback-oriented while reusing the mosquitto_nim highlevel client underneath.
#
# Compatibility timing rules:
# - publish() returns after the local highlevel client accepts the command into
#   the worker queue. It does not wait for PUBACK/PublishCompleted.
# - subscribe() registers the local callback and queues SUBSCRIBE. It does not
#   wait for SUBACK.
# - incoming messages are dispatched by an asyncdispatch-side pump; application
#   callbacks are never invoked from the libmosquitto worker thread.
# ------------------------------------------------------------------------------
type
  MqttCompatError* = object of CatchableError

  PubCallback* = proc(topic: string; message: string)
    ## nmqtt-compatible message callback.
    ##
    ## This callback is invoked on the asyncdispatch/application thread by the
    ## compatibility event pump, not from the libmosquitto worker thread.

  MqttCtx* = ref object
    clientId: string
    host: string
    port: int
    keepalive: int
    cleanSession: bool
    sslOn: bool
    username: string
    password: string
    sslCert: string
    sslKey: string
    willTopic: string
    willMessage: string
    willQos: int
    willRetain: bool
    connectTimeoutMs: int
    pumpSleepMs: int
    client: MqttClient
    started: bool
    running: bool
    connected: bool
    stopped: bool
    pendingCount: int
    subscriptions: seq[MqttSubscription]
    lastError: Option[MqttError]

# ------------------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------------------
proc setLastError(ctx: MqttCtx; error: MqttError) =
  if not ctx.isNil:
    ctx.lastError = some(error)

proc raiseCompat(ctx: MqttCtx; error: MqttError) {.noinline.} =
  ctx.setLastError(error)
  raise newException(MqttCompatError, $error)

proc requireCtx(ctx: MqttCtx; context: string) =
  if ctx.isNil:
    raise newException(MqttCompatError, $invalidState(context, "context is nil"))

proc qosFromInt(ctx: MqttCtx; qos: int; context: string): MqttQos =
  let qosRes = toMqttQos(qos, context)
  if qosRes.isErr:
    ctx.raiseCompat(qosRes.error)
  result = qosRes.get()

proc ensureSupportedConfig(ctx: MqttCtx) =
  ## Keep unsupported nmqtt settings explicit instead of silently ignoring them.
  ## Lowlevel support can be wired later without changing the compatibility API.
  if ctx.sslOn:
    ctx.raiseCompat(invalidState("start nmqtt-compatible client", "SSL/TLS is not wired yet"))
  if ctx.sslCert.len > 0 or ctx.sslKey.len > 0:
    ctx.raiseCompat(invalidState("start nmqtt-compatible client", "client certificate auth is not wired yet"))
  if ctx.willTopic.len > 0:
    ctx.raiseCompat(invalidState("start nmqtt-compatible client", "will configuration is not wired yet"))

proc ensureClient(ctx: MqttCtx) =
  ctx.requireCtx("ensure nmqtt-compatible client")
  if not ctx.client.isNil and not ctx.client.isClosed:
    return

  ctx.ensureSupportedConfig()

  let clientRes = startMqttClient(
    clientId = ctx.clientId,
    cleanSession = ctx.cleanSession,
    loopTimeoutMs = 10,
    pollMs = max(ctx.pumpSleepMs, 1),
  )
  if clientRes.isErr:
    ctx.raiseCompat(clientRes.error)

  ctx.client = clientRes.get()
  ctx.started = true
  ctx.running = false
  ctx.connected = false
  ctx.stopped = false

proc handleCompatEvent(ctx: MqttCtx; event: MqttEvent): Future[void] {.async.} =
  case event.kind
  of mevConnected:
    ctx.connected = true
  of mevDisconnected:
    ctx.connected = false
  of mevPublishCompleted, mevSubscribed, mevUnsubscribed:
    if ctx.pendingCount > 0:
      dec ctx.pendingCount
  of mevStopped:
    ctx.running = false
    ctx.stopped = true
  of mevError:
    ctx.setLastError(event.error)
  else:
    discard

  if event.kind == mevMessageReceived and not ctx.client.isNil:
    let dispatchRes = await ctx.client.dispatchEvent(event)
    if dispatchRes.isErr:
      ctx.setLastError(dispatchRes.error)

proc pumpEvents(ctx: MqttCtx): Future[void] {.async.} =
  while ctx.running and not ctx.client.isNil and not ctx.client.isClosed:
    let eventRes = await ctx.client.nextEvent()
    if eventRes.isErr:
      ctx.setLastError(eventRes.error)
      await sleepAsync(ctx.pumpSleepMs)
      continue

    await ctx.handleCompatEvent(eventRes.get())

proc waitForControlEvent(ctx: MqttCtx; commandId: int; kind: MqttEventKind;
                         timeoutMs: int): Future[bool] {.async.} =
  if ctx.client.isNil:
    return false

  let sleepMs = 10
  let rounds = max(1, timeoutMs div sleepMs)
  for _ in 0 ..< rounds:
    let drainRes = ctx.client.drainEvents()
    if drainRes.isErr:
      ctx.setLastError(drainRes.error)
      return false

    for event in drainRes.get():
      await ctx.handleCompatEvent(event)
      if event.kind == kind and event.commandId == commandId:
        return true

    await sleepAsync(sleepMs)

  result = false

# ------------------------------------------------------------------------------
# nmqtt-compatible configuration API
# ------------------------------------------------------------------------------
proc newMqttCtx*(clientId: string): MqttCtx =
  new result
  result.clientId = clientId
  result.host = "127.0.0.1"
  result.port = 1883
  result.keepalive = 60
  result.cleanSession = true
  result.sslOn = false
  result.connectTimeoutMs = 5000
  result.pumpSleepMs = 1
  result.pendingCount = 0
  result.subscriptions = @[]
  result.lastError = none(MqttError)

proc set_ping_interval*(ctx: MqttCtx; txInterval: int) =
  ctx.requireCtx("set MQTT ping interval")
  if txInterval <= 0:
    ctx.raiseCompat(invalidArgument("set MQTT ping interval", "interval must be positive"))
  ctx.keepalive = txInterval

proc set_host*(ctx: MqttCtx; host: string; port: int = 1883; sslOn = false) =
  ctx.requireCtx("set MQTT host")
  if host.len == 0:
    ctx.raiseCompat(invalidArgument("set MQTT host", "host must not be empty"))
  if port <= 0 or port > 65535:
    ctx.raiseCompat(invalidArgument("set MQTT host", "port must be in 1..65535"))
  ctx.host = host
  ctx.port = port
  ctx.sslOn = sslOn

proc set_auth*(ctx: MqttCtx; username: string; password: string) =
  ctx.requireCtx("set MQTT auth")
  ctx.username = username
  ctx.password = password

proc set_ssl_certificates*(ctx: MqttCtx; sslCert: string; sslKey: string) =
  ctx.requireCtx("set MQTT SSL certificates")
  ctx.sslCert = sslCert
  ctx.sslKey = sslKey

proc set_will*(ctx: MqttCtx; topic, msg: string; qos = 0; retain = false) =
  ctx.requireCtx("set MQTT will")
  discard ctx.qosFromInt(qos, "set MQTT will")
  ctx.willTopic = topic
  ctx.willMessage = msg
  ctx.willQos = qos
  ctx.willRetain = retain

proc set_connect_timeout*(ctx: MqttCtx; timeoutMs: int) =
  ## Compatibility helper for tests/applications that want a shorter connect wait.
  ctx.requireCtx("set MQTT connect timeout")
  if timeoutMs <= 0:
    ctx.raiseCompat(invalidArgument("set MQTT connect timeout", "timeout must be positive"))
  ctx.connectTimeoutMs = timeoutMs

proc lastError*(ctx: MqttCtx): Option[MqttError] =
  if ctx.isNil:
    return none(MqttError)
  result = ctx.lastError

# ------------------------------------------------------------------------------
# nmqtt-compatible lifecycle API
# ------------------------------------------------------------------------------
proc connect*(ctx: MqttCtx) {.async.} =
  ctx.requireCtx("connect MQTT client")
  if ctx.connected:
    return

  ctx.ensureClient()
  let connectRes = ctx.client.connect(
    ctx.host,
    port = ctx.port,
    keepalive = ctx.keepalive,
    username = ctx.username,
    password = ctx.password
  )
  if connectRes.isErr:
    ctx.raiseCompat(connectRes.error)

  let connected = await ctx.waitForControlEvent(connectRes.get(), mevConnected, ctx.connectTimeoutMs)
  if not connected:
    ctx.raiseCompat(invalidState("connect MQTT client", "connect timeout"))

proc start*(ctx: MqttCtx) {.async.} =
  ## Connect and start the asyncdispatch-side compatibility event pump.
  ##
  ## This is the nmqtt-style entry point.  Automatic reconnect is intentionally
  ## not implemented in this first compatibility step; the API shape is kept so
  ## reconnect policy can be added behind the same surface later.
  ctx.requireCtx("start MQTT client")
  if ctx.running:
    return

  await ctx.connect()
  ctx.running = true
  ctx.stopped = false
  asyncCheck ctx.pumpEvents()

proc disconnect*(ctx: MqttCtx) {.async.} =
  ctx.requireCtx("disconnect MQTT client")
  if ctx.client.isNil:
    ctx.connected = false
    ctx.running = false
    ctx.started = false
    return

  if ctx.connected:
    let disconnectRes = ctx.client.disconnect()
    if disconnectRes.isErr:
      ctx.raiseCompat(disconnectRes.error)

  let stopRes = ctx.client.requestStop()
  if stopRes.isErr:
    ctx.raiseCompat(stopRes.error)

  for _ in 0 ..< 500:
    if ctx.stopped:
      break
    await sleepAsync(10)

  ctx.running = false
  let joinRes = ctx.client.joinMqttClient()
  if joinRes.isErr:
    ctx.raiseCompat(joinRes.error)

  ctx.connected = false
  ctx.started = false
  ctx.client = nil
  ctx.subscriptions.setLen(0)

# ------------------------------------------------------------------------------
# nmqtt-compatible message API
# ------------------------------------------------------------------------------
proc publish*(ctx: MqttCtx; topic: string; message: string; qos = 0;
              retain = false) {.async.} =
  ctx.requireCtx("publish MQTT message")
  if not ctx.started or ctx.client.isNil:
    await ctx.start()

  let mqttQos = ctx.qosFromInt(qos, "publish MQTT message")
  let publishRes = ctx.client.publish(topic, message, mqttQos, retain = retain)
  if publishRes.isErr:
    ctx.raiseCompat(publishRes.error)

  # Keep nmqtt-style queue-oriented semantics.  Do not wait for PUBACK here.
  if mqttQos != qos0:
    inc ctx.pendingCount

proc subscribe*(ctx: MqttCtx; topic: string; qos: int;
                callback: PubCallback): Future[void] {.async.} =
  ctx.requireCtx("subscribe MQTT topic")
  if callback.isNil:
    ctx.raiseCompat(invalidArgument("subscribe MQTT topic", "callback is nil"))
  if not ctx.started or ctx.client.isNil:
    await ctx.start()

  let mqttQos = ctx.qosFromInt(qos, "subscribe MQTT topic")
  let handler: MqttMessageHandler = proc(message: MqttMessage) =
    callback(message.topic, message.payloadString())

  let subRes = ctx.client.subscribe(topic, mqttQos, handler)
  if subRes.isErr:
    ctx.raiseCompat(subRes.error)

  ctx.subscriptions.add(subRes.get())
  inc ctx.pendingCount

proc unsubscribe*(ctx: MqttCtx; topic: string): Future[void] {.async.} =
  ctx.requireCtx("unsubscribe MQTT topic")
  if ctx.client.isNil:
    return

  var removed = false
  var i = 0
  while i < ctx.subscriptions.len:
    if ctx.subscriptions[i].topicFilter == topic:
      let unsubRes = ctx.client.unsubscribe(ctx.subscriptions[i])
      if unsubRes.isErr:
        ctx.raiseCompat(unsubRes.error)
      ctx.subscriptions.delete(i)
      inc ctx.pendingCount
      removed = true
    else:
      inc i

  if not removed:
    let unsubRes = ctx.client.unsubscribe(topic)
    if unsubRes.isErr:
      ctx.raiseCompat(unsubRes.error)
    inc ctx.pendingCount

proc isConnected*(ctx: MqttCtx): bool =
  result = not ctx.isNil and ctx.connected

proc msgQueue*(ctx: MqttCtx): int =
  if ctx.isNil:
    return 0
  result = ctx.pendingCount
