# Destination: src/mosquitto_nim/lowlevel/types.nim

import std/strformat

import results

import ./errors

type
  MqttProtocolVersion* = enum
    mpv311 = 4
    mpv5 = 5

  MqttQos* = enum
    qos0 = 0
    qos1 = 1
    qos2 = 2

  MqttVersion* = object
    major*: int
    minor*: int
    revision*: int

  MqttRetain* = bool

  MqttPropertyKind* = enum
    mpUnknown

  MqttProperty* = object
    kind*: MqttPropertyKind

  MqttProperties* = seq[MqttProperty]

  MqttMessage* = object
    ## Nim-owned MQTT message.
    ##
    ## C callback data from libmosquitto must be copied into this type before it
    ## crosses module/thread boundaries. Raw C pointers must not escape through
    ## this object.
    mid*: int
    topic*: string
    payload*: seq[byte]
    qos*: MqttQos
    retain*: bool
    dup*: bool
    properties*: MqttProperties

# ------------------------------------------------------------------------------
# Formatting / conversion helpers
# ------------------------------------------------------------------------------
proc `$`*(version: MqttVersion): string =
  result = &"{version.major}.{version.minor}.{version.revision}"

proc toInt*(protocolVersion: MqttProtocolVersion): int =
  result = ord(protocolVersion)

proc toInt*(qos: MqttQos): int =
  result = ord(qos)

proc toMqttQos*(value: int; context = "MQTT QoS"): MqttResult[MqttQos] =
  case value
  of 0:
    result = ok(qos0)
  of 1:
    result = ok(qos1)
  of 2:
    result = ok(qos2)
  else:
    result = err(invalidArgument(context, &"QoS must be 0, 1, or 2: {value}"))

proc payloadString*(message: MqttMessage): string =
  ## Return payload bytes as a Nim string.
  ##
  ## This is a convenience helper for text payloads and nmqtt-compatible APIs.
  ## Binary payloads should use `message.payload` directly.
  if message.payload.len == 0:
    return ""

  result = newString(message.payload.len)
  copyMem(addr result[0], unsafeAddr message.payload[0], message.payload.len)
