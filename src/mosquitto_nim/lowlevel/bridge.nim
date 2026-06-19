# Destination: src/mosquitto_nim/lowlevel/bridge.nim

import std/[options, strformat]

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
  MqttPropPayloadFormatIndicatorId* = 1
    ## MQTT v5 Payload Format Indicator identifier (0x01).
  MqttPropMessageExpiryIntervalId* = 2
    ## MQTT v5 Message Expiry Interval identifier (0x02).
  MqttPropContentTypeId* = 3
    ## MQTT v5 Content Type identifier (0x03).
  MqttPropResponseTopicId* = 8
    ## MQTT v5 Response Topic identifier (0x08).
  MqttPropCorrelationDataId* = 9
    ## MQTT v5 Correlation Data identifier (0x09).
  MqttPropSessionExpiryIntervalId* = 17
    ## MQTT v5 Session Expiry Interval identifier (0x11).
  MqttPropAssignedClientIdentifierId* = 18
    ## MQTT v5 Assigned Client Identifier identifier (0x12).
  MqttPropServerKeepAliveId* = 19
    ## MQTT v5 Server Keep Alive identifier (0x13).
  MqttPropRequestProblemInformationId* = 23
    ## MQTT v5 Request Problem Information identifier (0x17).
  MqttPropResponseInformationId* = 26
    ## MQTT v5 Response Information identifier (0x1A).
  MqttPropServerReferenceId* = 28
    ## MQTT v5 Server Reference identifier (0x1C).
  MqttPropReasonStringId* = 31
    ## MQTT v5 Reason String identifier (0x1F).
  MqttPropReceiveMaximumId* = 33
    ## MQTT v5 Receive Maximum identifier (0x21).
  MqttPropUserPropertyId* = 38
    ## MQTT v5 User Property identifier (0x26).
  MqttPropMaximumPacketSizeId* = 39
    ## MQTT v5 Maximum Packet Size identifier (0x27).

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

proc copyByteProperty(properties: ptr mosquitto_property; identifier: cint;
                      makeProperty: proc(value: uint8): MqttProperty;
                      context: string;
                      copied: var MqttProperties): MqttResult[MqttOk] =
  ## Copy byte MQTT v5 properties into Nim-owned MqttProperty values.
  var cursor = properties
  var skipFirst = false
  while true:
    var value: uint8 = 0
    let found = mosquitto_property_read_byte(
      cursor,
      identifier,
      addr value,
      skipFirst
    )
    if found == nil:
      break

    copied.add(makeProperty(value))
    cursor = found
    skipFirst = true

  result = ok(MqttOk())

proc copyInt16Property(properties: ptr mosquitto_property; identifier: cint;
                       makeProperty: proc(value: uint16): MqttProperty;
                       context: string;
                       copied: var MqttProperties): MqttResult[MqttOk] =
  ## Copy 16-bit integer MQTT v5 properties into Nim-owned MqttProperty values.
  var cursor = properties
  var skipFirst = false
  while true:
    var value: uint16 = 0
    let found = mosquitto_property_read_int16(
      cursor,
      identifier,
      addr value,
      skipFirst
    )
    if found == nil:
      break

    copied.add(makeProperty(value))
    cursor = found
    skipFirst = true

  result = ok(MqttOk())

proc copyInt32Property(properties: ptr mosquitto_property; identifier: cint;
                       makeProperty: proc(value: uint32): MqttProperty;
                       context: string;
                       copied: var MqttProperties): MqttResult[MqttOk] =
  ## Copy 32-bit integer MQTT v5 properties into Nim-owned MqttProperty values.
  var cursor = properties
  var skipFirst = false
  while true:
    var value: uint32 = 0
    let found = mosquitto_property_read_int32(
      cursor,
      identifier,
      addr value,
      skipFirst
    )
    if found == nil:
      break

    copied.add(makeProperty(value))
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

proc copyPayloadFormatIndicatorProperty(value: uint8): MqttProperty =
  result = payloadFormatIndicator(value)

proc copyServerKeepAliveProperty(value: uint16): MqttProperty =
  result = serverKeepAlive(value)

proc copyReceiveMaximumProperty(value: uint16): MqttProperty =
  result = receiveMaximum(value)

proc copyProperties*(properties: ptr mosquitto_property): MqttResult[MqttProperties] =
  ## Copy supported MQTT v5 properties from a libmosquitto property list.
  ##
  ## All returned values are Nim-owned. Unsupported properties are intentionally
  ## ignored for now so the supported subset can grow without breaking callers.
  var copied: MqttProperties = @[]
  if properties == nil:
    return ok(copied)

  let payloadFormatRes = copyByteProperty(
    properties,
    MqttPropPayloadFormatIndicatorId.cint,
    copyPayloadFormatIndicatorProperty,
    "MQTT v5 Payload Format Indicator",
    copied
  )
  if payloadFormatRes.isErr:
    return err(payloadFormatRes.error)

  let expiryRes = copyInt32Property(
    properties,
    MqttPropMessageExpiryIntervalId.cint,
    messageExpiryInterval,
    "MQTT v5 Message Expiry Interval",
    copied
  )
  if expiryRes.isErr:
    return err(expiryRes.error)

  let contentTypeRes = copyStringProperty(
    properties,
    MqttPropContentTypeId.cint,
    contentType,
    "MQTT v5 Content Type",
    copied
  )
  if contentTypeRes.isErr:
    return err(contentTypeRes.error)

  let assignedClientRes = copyStringProperty(
    properties,
    MqttPropAssignedClientIdentifierId.cint,
    assignedClientIdentifier,
    "MQTT v5 Assigned Client Identifier",
    copied
  )
  if assignedClientRes.isErr:
    return err(assignedClientRes.error)

  let serverKeepAliveRes = copyInt16Property(
    properties,
    MqttPropServerKeepAliveId.cint,
    copyServerKeepAliveProperty,
    "MQTT v5 Server Keep Alive",
    copied
  )
  if serverKeepAliveRes.isErr:
    return err(serverKeepAliveRes.error)

  let receiveMaximumRes = copyInt16Property(
    properties,
    MqttPropReceiveMaximumId.cint,
    copyReceiveMaximumProperty,
    "MQTT v5 Receive Maximum",
    copied
  )
  if receiveMaximumRes.isErr:
    return err(receiveMaximumRes.error)

  let maximumPacketSizeRes = copyInt32Property(
    properties,
    MqttPropMaximumPacketSizeId.cint,
    maximumPacketSize,
    "MQTT v5 Maximum Packet Size",
    copied
  )
  if maximumPacketSizeRes.isErr:
    return err(maximumPacketSizeRes.error)

  let reasonStringRes = copyStringProperty(
    properties,
    MqttPropReasonStringId.cint,
    reasonString,
    "MQTT v5 Reason String",
    copied
  )
  if reasonStringRes.isErr:
    return err(reasonStringRes.error)

  let responseInformationRes = copyStringProperty(
    properties,
    MqttPropResponseInformationId.cint,
    responseInformation,
    "MQTT v5 Response Information",
    copied
  )
  if responseInformationRes.isErr:
    return err(responseInformationRes.error)

  let serverReferenceRes = copyStringProperty(
    properties,
    MqttPropServerReferenceId.cint,
    serverReference,
    "MQTT v5 Server Reference",
    copied
  )
  if serverReferenceRes.isErr:
    return err(serverReferenceRes.error)

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
    of mpMessageExpiryInterval:
      let rc = mosquitto_property_add_int32(
        addr raw,
        MqttPropMessageExpiryIntervalId.cint,
        property.intValue
      )
      let addRes = checkMosq(rc, "mosquitto_property_add_int32(Message Expiry Interval)")
      if addRes.isErr:
        mosquitto_property_free_all(addr raw)
        return err(addRes.error)
    of mpContentType:
      let rc = mosquitto_property_add_string(
        addr raw,
        MqttPropContentTypeId.cint,
        property.value.cstring
      )
      let addRes = checkMosq(rc, "mosquitto_property_add_string(Content Type)")
      if addRes.isErr:
        mosquitto_property_free_all(addr raw)
        return err(addRes.error)
    of mpPayloadFormatIndicator:
      let indicatorRes = toMqttPayloadFormatIndicator(property.intValue, context)
      if indicatorRes.isErr:
        mosquitto_property_free_all(addr raw)
        return err(indicatorRes.error)

      let rc = mosquitto_property_add_byte(
        addr raw,
        MqttPropPayloadFormatIndicatorId.cint,
        property.intValue.uint8
      )
      let addRes = checkMosq(rc, "mosquitto_property_add_byte(Payload Format Indicator)")
      if addRes.isErr:
        mosquitto_property_free_all(addr raw)
        return err(addRes.error)
    of mpAssignedClientIdentifier, mpServerKeepAlive, mpReceiveMaximum,
       mpMaximumPacketSize, mpReasonString, mpResponseInformation,
       mpServerReference:
      mosquitto_property_free_all(addr raw)
      return err(invalidArgument(context, &"{property.kind} is not valid for outgoing PUBLISH properties"))
    of mpUnknown:
      mosquitto_property_free_all(addr raw)
      return err(invalidArgument(context, "unknown MQTT property kind"))

  result = ok(raw)

proc freeMosquittoProperties*(properties: var ptr mosquitto_property) =
  ## Free a libmosquitto property list returned by buildMosquittoProperties().
  mosquitto_property_free_all(addr properties)

proc buildMosquittoConnectProperties*(properties: MqttConnectProperties;
                                      context = "MQTT v5 CONNECT properties"): MqttResult[ptr mosquitto_property] =
  ## Convert typed MQTT v5 CONNECT properties to a libmosquitto property list.
  ##
  ## The caller owns the returned property list and must free it with
  ## freeMosquittoProperties(), even when the subsequent libmosquitto call fails.
  let validateRes = validateConnectProperties(properties, context)
  if validateRes.isErr:
    return err(validateRes.error)

  var raw: ptr mosquitto_property = nil

  if properties.sessionExpiryInterval.isSome:
    let rc = mosquitto_property_add_int32(
      addr raw,
      MqttPropSessionExpiryIntervalId.cint,
      properties.sessionExpiryInterval.get()
    )
    let addRes = checkMosq(rc, "mosquitto_property_add_int32(Session Expiry Interval)")
    if addRes.isErr:
      mosquitto_property_free_all(addr raw)
      return err(addRes.error)

  if properties.receiveMaximum.isSome:
    let rc = mosquitto_property_add_int16(
      addr raw,
      MqttPropReceiveMaximumId.cint,
      properties.receiveMaximum.get()
    )
    let addRes = checkMosq(rc, "mosquitto_property_add_int16(Receive Maximum)")
    if addRes.isErr:
      mosquitto_property_free_all(addr raw)
      return err(addRes.error)

  if properties.maximumPacketSize.isSome:
    let rc = mosquitto_property_add_int32(
      addr raw,
      MqttPropMaximumPacketSizeId.cint,
      properties.maximumPacketSize.get()
    )
    let addRes = checkMosq(rc, "mosquitto_property_add_int32(Maximum Packet Size)")
    if addRes.isErr:
      mosquitto_property_free_all(addr raw)
      return err(addRes.error)

  if properties.requestProblemInformation.isSome:
    let value = if properties.requestProblemInformation.get(): 1'u8 else: 0'u8
    let rc = mosquitto_property_add_byte(
      addr raw,
      MqttPropRequestProblemInformationId.cint,
      value
    )
    let addRes = checkMosq(rc, "mosquitto_property_add_byte(Request Problem Information)")
    if addRes.isErr:
      mosquitto_property_free_all(addr raw)
      return err(addRes.error)

  for item in properties.userProperties:
    let rc = mosquitto_property_add_string_pair(
      addr raw,
      MqttPropUserPropertyId.cint,
      item[0].cstring,
      item[1].cstring
    )
    let addRes = checkMosq(rc, "mosquitto_property_add_string_pair(CONNECT User Property)")
    if addRes.isErr:
      mosquitto_property_free_all(addr raw)
      return err(addRes.error)

  result = ok(raw)

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
