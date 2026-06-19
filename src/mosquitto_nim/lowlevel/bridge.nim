# Destination: src/mosquitto_nim/lowlevel/bridge.nim

import results

import ./bindings/c_api
import ./errors
import ./types

# ------------------------------------------------------------------------------
# C -> Nim owned data conversion helpers.
# ------------------------------------------------------------------------------
proc copyPayload*(payload: pointer; payloadLen: cint;
                  context = "mosquitto message payload"): MqttResult[seq[byte]] =
  if payloadLen < 0.cint:
    return err(invalidArgument(context, "payload length must not be negative"))

  if payloadLen == 0.cint:
    return ok(newSeq[byte](0))

  if payload == nil:
    return err(invalidArgument(context, "payload pointer is nil but payload length is non-zero"))

  var copied = newSeq[byte](payloadLen.int)
  copyMem(addr copied[0], payload, payloadLen.int)
  result = ok(copied)

proc copyMessage*(message: ptr struct_mosquitto_message;
                  properties: ptr mosquitto_property = nil): MqttResult[MqttMessage] =
  ## Copy a libmosquitto message into Nim-owned memory.
  ##
  ## The topic/payload pointers passed by libmosquitto are treated as callback
  ## scoped. This function must copy them before the callback returns.
  discard properties

  if message == nil:
    return err(invalidArgument("mosquitto message", "message pointer is nil"))

  if message.topic == nil:
    return err(invalidArgument("mosquitto message", "topic pointer is nil"))

  let qosRes = toMqttQos(message.qos.int, "mosquitto message qos")
  if qosRes.isErr:
    return err(qosRes.error)

  let payloadRes = copyPayload(message.payload, message.payloadlen)
  if payloadRes.isErr:
    return err(payloadRes.error)

  result = ok(MqttMessage(
    mid: message.mid.int,
    topic: $message.topic,
    payload: payloadRes.get(),
    qos: qosRes.get(),
    retain: message.retain,
    dup: false,
    properties: @[]
  ))
