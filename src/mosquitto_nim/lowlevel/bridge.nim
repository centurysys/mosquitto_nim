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
  MqttPropResponseTopicId* = 8
    ## MQTT v5 Response Topic identifier (0x08).
  MqttPropCorrelationDataId* = 9
    ## MQTT v5 Correlation Data identifier (0x09).
  MqttPropUserPropertyId* = 38
    ## MQTT v5 User Property identifier (0x26).

proc copyUserProperties(properties: ptr mosquitto_property;
                        copied: var MqttProperties): MqttResult[MqttOk] =
  ## Copy all MQTT v5 User Properties from a libmosquitto property list.
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

  result = ok(MqttOk())

proc copyStringProperty(properties: ptr mosquitto_property; identifier: cint;
                        makeProperty: proc(value: string): MqttProperty;
                        context: string;
                        copied: var MqttProperties): MqttResult[MqttOk] =
  ## Copy string MQTT v5 properties into Nim-owned MqttProperty values.
  var cursor = properties
  var skipFirst = false
  while true:
    var value: cstring = nil
    let found = mosquitto_property_read_string(
      cursor,
      identifier,
      addr value,
      skipFirst
    )
    if found == nil:
      break

    if value == nil:
      return err(invalidArgument(context, "property value is nil"))

    copied.add(makeProperty($value))
    cursor = found
    skipFirst = true

  result = ok(MqttOk())

proc copyBinaryProperty(properties: ptr mosquitto_property; identifier: cint;
                        makeProperty: proc(data: openArray[byte]): MqttProperty;
                        context: string;
                        copied: var MqttProperties): MqttResult[MqttOk] =
  ## Copy binary MQTT v5 properties into Nim-owned MqttProperty values.
  var cursor = properties
  var skipFirst = false
  while true:
    var value: pointer = nil
    var valueLen: uint16 = 0
    let found = mosquitto_property_read_binary(
      cursor,
      identifier,
      addr value,
      addr valueLen,
      skipFirst
    )
    if found == nil:
      break

    if valueLen > 0'u16 and value == nil:
      return err(invalidArgument(context, "property value is nil but length is non-zero"))

    var data = newSeq[byte](valueLen.int)
    if valueLen > 0'u16:
      copyMem(addr data[0], value, valueLen.int)

    copied.add(makeProperty(data))
    cursor = found
    skipFirst = true

  result = ok(MqttOk())

proc copyProperties*(properties: ptr mosquitto_property): MqttResult[MqttProperties] =
  ## Copy supported MQTT v5 properties from a libmosquitto property list.
  ##
  ## All returned values are Nim-owned. Unsupported properties are intentionally
  ## ignored for now so the supported subset can grow without breaking callers.
  var copied: MqttProperties = @[]
  if properties == nil:
    return ok(copied)

  let userRes = copyUserProperties(properties, copied)
  if userRes.isErr:
    return err(userRes.error)

  let responseRes = copyStringProperty(
    properties,
    MqttPropResponseTopicId.cint,
    responseTopic,
    "MQTT v5 Response Topic",
    copied
  )
  if responseRes.isErr:
    return err(responseRes.error)

  let correlationRes = copyBinaryProperty(
    properties,
    MqttPropCorrelationDataId.cint,
    correlationData,
    "MQTT v5 Correlation Data",
    copied
  )
  if correlationRes.isErr:
    return err(correlationRes.error)

  result = ok(copied)

proc copyUserProperties*(properties: ptr mosquitto_property): MqttResult[MqttProperties] =
  ## Backward-compatible helper that currently copies all supported properties.
  ##
  ## Older code/tests used this function name while only User Property was
  ## supported.  Keep it as an alias so callers get newly supported properties
  ## without changing the lowlevel bridge call site.
  result = copyProperties(properties)

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
    of mpResponseTopic:
      if property.value.len == 0:
        mosquitto_property_free_all(addr raw)
        return err(invalidArgument(context, "Response Topic must not be empty"))

      let topicRes = checkMosq(
        mosquitto_pub_topic_check(property.value.cstring),
        "mosquitto_pub_topic_check(Response Topic)"
      )
      if topicRes.isErr:
        mosquitto_property_free_all(addr raw)
        return err(topicRes.error)

      let rc = mosquitto_property_add_string(
        addr raw,
        MqttPropResponseTopicId.cint,
        property.value.cstring
      )
      let addRes = checkMosq(rc, "mosquitto_property_add_string(Response Topic)")
      if addRes.isErr:
        mosquitto_property_free_all(addr raw)
        return err(addRes.error)
    of mpCorrelationData:
      if property.data.len > high(uint16).int:
        mosquitto_property_free_all(addr raw)
        return err(invalidArgument(context, "Correlation Data must be at most 65535 bytes"))

      var dataPtr: pointer = nil
      if property.data.len > 0:
        dataPtr = cast[pointer](unsafeAddr property.data[0])

      let rc = mosquitto_property_add_binary(
        addr raw,
        MqttPropCorrelationDataId.cint,
        dataPtr,
        property.data.len.uint16
      )
      let addRes = checkMosq(rc, "mosquitto_property_add_binary(Correlation Data)")
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

  let propertiesRes = copyProperties(properties)
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
