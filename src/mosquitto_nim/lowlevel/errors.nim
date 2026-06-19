# Destination: src/mosquitto_nim/lowlevel/errors.nim

import std/strformat

import results

import ./bindings/c_api

type
  MqttErrorKind* = enum
    meLibraryError
    meInvalidArgument
    meInvalidState
    meTimeout
    meQueueClosed
    meQueueOverflow
    meProtocolReason

  MqttError* = object
    kind*: MqttErrorKind
    code*: int
    context*: string
    message*: string

  MqttOk* = object

  MqttResult*[T] = Result[T, MqttError]

# ------------------------------------------------------------------------------
# Error construction / formatting
# ------------------------------------------------------------------------------
proc makeError*(kind: MqttErrorKind; context, message: string; code = 0): MqttError =
  result = MqttError(
    kind: kind,
    code: code,
    context: context,
    message: message
  )

proc `$`*(error: MqttError): string =
  if error.code != 0:
    result = &"{error.context}: {error.message} (kind={error.kind}, code={error.code})"
  else:
    result = &"{error.context}: {error.message} (kind={error.kind})"

proc invalidArgument*(context, message: string): MqttError =
  result = makeError(meInvalidArgument, context, message)

proc invalidState*(context, message: string): MqttError =
  result = makeError(meInvalidState, context, message)

# ------------------------------------------------------------------------------
# libmosquitto error helpers
# ------------------------------------------------------------------------------
proc mqttStrError*(code: int): string =
  let cmsg = mosquitto_strerror(cint(code))
  if cmsg == nil:
    return &"unknown libmosquitto error: {code}"

  result = $cmsg

proc mqttErrorFromMosq*(rc: cint; context = "libmosquitto"): MqttError =
  result = makeError(
    meLibraryError,
    context,
    mqttStrError(rc.int),
    rc.int
  )

proc checkMosq*(rc: cint; context = "libmosquitto"): MqttResult[MqttOk] =
  if rc == 0.cint:
    return ok(MqttOk())

  result = err(mqttErrorFromMosq(rc, context))
