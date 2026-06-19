# Destination: src/mosquitto_nim/lowlevel/client.nim

import results

import ./bindings/c_api
import ./errors

# ------------------------------------------------------------------------------
# Minimal low-level client handle wrapper.
#
# This layer owns a libmosquitto client handle, but still does not perform MQTT
# network I/O. Connection/publish/subscribe wrappers are added in a later step.
# ------------------------------------------------------------------------------
type
  LowLevelClient* = ref object
    raw: ptr struct_mosquitto

proc newLowLevelClient*(clientId: string; cleanSession = true;
                        userdata: pointer = nil): MqttResult[LowLevelClient] =
  ## Create a libmosquitto client handle.
  ##
  ## An empty clientId is passed as nil, allowing libmosquitto to generate a
  ## client id where supported. The raw handle is intentionally kept private.
  var cClientId: cstring = nil
  if clientId.len > 0:
    cClientId = clientId.cstring

  let raw = mosquitto_new(cClientId, cleanSession, userdata)
  if raw == nil:
    return err(makeError(
      meLibraryError,
      "mosquitto_new",
      "failed to create libmosquitto client handle"
    ))

  result = ok(LowLevelClient(raw: raw))

proc isClosed*(client: LowLevelClient): bool =
  result = client.isNil or client.raw == nil

proc closeLowLevelClient*(client: LowLevelClient): MqttResult[MqttOk] =
  ## Destroy the libmosquitto client handle.
  ##
  ## This operation is intentionally idempotent so cleanup paths can call it
  ## safely after partial initialization failures.
  if client.isNil or client.raw == nil:
    return ok(MqttOk())

  mosquitto_destroy(client.raw)
  client.raw = nil
  result = ok(MqttOk())
