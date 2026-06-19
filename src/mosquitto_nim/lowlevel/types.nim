# Destination: src/mosquitto_nim/lowlevel/types.nim

import std/strformat

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

# ------------------------------------------------------------------------------
# Formatting / conversion helpers
# ------------------------------------------------------------------------------
proc `$`*(version: MqttVersion): string =
  result = &"{version.major}.{version.minor}.{version.revision}"

proc toInt*(protocolVersion: MqttProtocolVersion): int =
  result = ord(protocolVersion)

proc toInt*(qos: MqttQos): int =
  result = ord(qos)
