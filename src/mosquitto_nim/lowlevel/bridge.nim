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

# ------------------------------------------------------------------------------
# MQTT v5 property helpers.
# ------------------------------------------------------------------------------
const
  MqttPropUserPropertyId* = 38
    ## MQTT v5 User Property identifier (0x26).

proc copyUserProperties*(properties: ptr mosquitto_property): MqttResult[MqttProperties] =
  ## Copy MQTT v5 User Properties from a libmosquitto property list.
  ##
  ## The C strings returned by libmosquitto are treated as property-list owned and
  ## copied immediately into Nim-owned MqttProperty values.
  var copied: MqttProperties = @[]
  if properties == nil:
    return ok(copied)

  var cursor = properties
  var skipFirst = false
  while true:
    var name: cstring = nil
    var value: cstring = nil
    let found = mosquitto_property_read_string_pair(
      cursor,
      MqttPropUserPropertyId.cint,
      addr name,
      addr value,
      skipFirst
    )
    if found == nil:
      break

    if name == nil:
      return err(invalidArgument("MQTT v5 User Property", "property name is nil"))
    if value == nil:
      return err(invalidArgument("MQTT v5 User Property", "property value is nil"))

    copied.add(userProperty($name, $value))
    cursor = found
    skipFirst = true

  result = ok(copied)

proc buildMosquittoProperties*(properties: MqttProperties;
                               context = "MQTT v5 properties"): MqttResult[ptr mosquitto_property] =
  ## Convert Nim-owned MQTT v5 properties to a libmosquitto property list.
  ##
  ## The caller owns the returned property list and must free it with
  ## freeMosquittoProperties(), even when the subsequent libmosquitto call fails.
  var raw: ptr mosquitto_property = nil

  for property in properties:
    case property.kind
    of mpUserProperty:
      if property.name.len == 0:
        mosquitto_property_free_all(addr raw)
        return err(invalidArgument(context, "User Property name must not be empty"))

      let rc = mosquitto_property_add_string_pair(
        addr raw,
        MqttPropUserPropertyId.cint,
        property.name.cstring,
        property.value.cstring
      )
      let addRes = checkMosq(rc, "mosquitto_property_add_string_pair")
      if addRes.isErr:
        mosquitto_property_free_all(addr raw)
        return err(addRes.error)
    of mpUnknown:
      mosquitto_property_free_all(addr raw)
      return err(invalidArgument(context, "unknown MQTT property kind"))

  result = ok(raw)

proc freeMosquittoProperties*(properties: var ptr mosquitto_property) =
  ## Free a libmosquitto property list returned by buildMosquittoProperties().
  mosquitto_property_free_all(addr properties)

proc copyMessage*(message: ptr struct_mosquitto_message;
                  properties: ptr mosquitto_property = nil): MqttResult[MqttMessage] =
  ## Copy a libmosquitto message into Nim-owned memory.
  ##
  ## The topic/payload pointers passed by libmosquitto are treated as callback
  ## scoped. This function must copy them before the callback returns.
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

  let propertiesRes = copyUserProperties(properties)
  if propertiesRes.isErr:
    return err(propertiesRes.error)

  result = ok(MqttMessage(
    mid: message.mid.int,
    topic: $message.topic,
    payload: payloadRes.get(),
    qos: qosRes.get(),
    retain: message.retain,
    dup: false,
    properties: propertiesRes.get()
  ))
