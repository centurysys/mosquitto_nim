# Destination: src/mosquitto_nim/lowlevel/types.nim

import std/[options, strformat]

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

  MqttTlsConfig* = object
    ## Nim-owned TLS configuration for libmosquitto.
    ##
    ## The strings are kept in Nim-owned memory until the worker applies them to
    ## libmosquitto before connecting.  Empty strings are passed as nil so the
    ## caller can use only the pieces needed by its broker setup.
    enabled*: bool
    cafile*: string
    capath*: string
    certfile*: string
    keyfile*: string
    insecure*: bool

  MqttWill* = object
    ## Nim-owned MQTT Will configuration.
    ##
    ## The Will payload is stored in Nim-owned bytes until a lower layer applies
    ## it to libmosquitto before connecting.
    enabled*: bool
    topic*: string
    payload*: seq[byte]
    qos*: MqttQos
    retain*: bool

  MqttPropertyKind* = enum
    mpUnknown
    mpUserProperty
    mpResponseTopic
    mpCorrelationData
    mpMessageExpiryInterval
    mpContentType
    mpPayloadFormatIndicator

  MqttPayloadFormatIndicator* = enum
    ## MQTT v5 Payload Format Indicator values.
    ##
    ## 0 means unspecified/binary payload. 1 means the payload is UTF-8 encoded
    ## character data. Other values are invalid for MQTT v5.
    mpfiUnspecified = 0
    mpfiUtf8 = 1

  MqttProperty* = object
    ## Nim-owned MQTT v5 property.
    ##
    ## This property model intentionally stores values in Nim-owned memory.
    ## User Property uses name/value; string properties such as Response Topic
    ## and Content Type use value; Correlation Data uses data; integer/byte
    ## properties such as Message Expiry Interval and Payload Format Indicator
    ## use intValue. Raw libmosquitto property pointers must not escape into this
    ## type.
    kind*: MqttPropertyKind
    name*: string
    value*: string
    data*: seq[byte]
    intValue*: uint32

  MqttProperties* = seq[MqttProperty]

  MqttConnectProperties* = object
    ## Typed MQTT v5 CONNECT properties.
    ##
    ## This is an API-level container for future CONNECT property plumbing.
    ## Existing connect paths still do not send MQTT v5 CONNECT properties yet.
    sessionExpiryInterval*: Option[uint32]
    receiveMaximum*: Option[uint16]
    maximumPacketSize*: Option[uint32]
    userProperties*: seq[(string, string)]

  MqttPublishProperties* = object
    ## Typed MQTT v5 PUBLISH properties.
    ##
    ## This separates properties that are valid for PUBLISH packets from the
    ## lower-level generic MqttProperties representation.  Conversion back to
    ## MqttProperties happens at the worker/lowlevel boundary so existing code
    ## can continue using the generic representation.
    userProperties*: seq[(string, string)]
    responseTopic*: Option[string]
    correlationData*: Option[seq[byte]]
    messageExpiryInterval*: Option[uint32]
    contentType*: Option[string]
    payloadFormatIndicator*: Option[MqttPayloadFormatIndicator]

  MqttSubscribeProperties* = object
    ## Typed MQTT v5 SUBSCRIBE properties.
    ##
    ## This is an API-level container for future SUBSCRIBE property plumbing.
    ## Existing subscribe paths still do not send MQTT v5 SUBSCRIBE properties
    ## yet.
    subscriptionIdentifier*: Option[int]
    userProperties*: seq[(string, string)]

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

  LowLevelControlEventKind* = enum
    lleConnected
    lleDisconnected
    llePublishCompleted
    lleSubscribed
    lleUnsubscribed

  LowLevelControlEvent* = object
    ## Nim-owned control/ack callback event from libmosquitto.
    ##
    ## These events model callback notifications such as CONNACK, DISCONNECT,
    ## PUBACK/PUBCOMP completion, SUBACK, and UNSUBACK.  They are intentionally
    ## kept at the lowlevel boundary so higher layers can decide which timing
    ## semantics to expose.
    kind*: LowLevelControlEventKind
    mid*: int
    reasonCode*: int
    flags*: int
    grantedQos*: seq[int]
    properties*: MqttProperties

  MessageSink* = proc(message: MqttMessage)
    ## Low-level message sink used by the libmosquitto callback trampoline.
    ##
    ## The sink is called from whichever thread drives `loopLowLevelClient()`.
    ## Higher layers must not use this as an application callback boundary;
    ## worker/async layers should use it only to enqueue copied messages.

  ControlSink* = proc(event: LowLevelControlEvent)
    ## Low-level control sink used by libmosquitto ack/state callback trampolines.
    ##
    ## This is not an application callback boundary.  Worker/highlevel layers use
    ## it to translate libmosquitto notifications into worker events.

# ------------------------------------------------------------------------------
# Formatting / conversion helpers
# ------------------------------------------------------------------------------
proc `$`*(version: MqttVersion): string =
  result = &"{version.major}.{version.minor}.{version.revision}"

proc toInt*(protocolVersion: MqttProtocolVersion): int =
  result = ord(protocolVersion)

proc `$`*(protocolVersion: MqttProtocolVersion): string =
  case protocolVersion
  of mpv311:
    result = "MQTT 3.1.1"
  of mpv5:
    result = "MQTT 5"

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

proc toMqttPayloadFormatIndicator*(value: uint32;
                                   context = "MQTT Payload Format Indicator"): MqttResult[MqttPayloadFormatIndicator] =
  ## Convert a raw MQTT v5 Payload Format Indicator value.
  if value == 0'u32:
    return ok(mpfiUnspecified)
  if value == 1'u32:
    return ok(mpfiUtf8)

  result = err(invalidArgument(context, &"Payload Format Indicator must be 0 or 1: {value}"))

proc toInt*(indicator: MqttPayloadFormatIndicator): int =
  result = ord(indicator)


proc bytesFromString*(payload: string): seq[byte] =
  ## Convert a Nim string to MQTT payload bytes.
  if payload.len == 0:
    return @[]

  result = newSeq[byte](payload.len)
  copyMem(addr result[0], unsafeAddr payload[0], payload.len)

proc noTls*(): MqttTlsConfig =
  result = MqttTlsConfig(enabled: false)

proc mqttTls*(cafile = ""; capath = ""; certfile = ""; keyfile = "";
              insecure = false): MqttTlsConfig =
  result = MqttTlsConfig(
    enabled: true,
    cafile: cafile,
    capath: capath,
    certfile: certfile,
    keyfile: keyfile,
    insecure: insecure
  )

proc noWill*(): MqttWill =
  result = MqttWill(enabled: false, qos: qos0)

proc mqttWill*(topic: string; payload: openArray[byte]; qos = qos0;
               retain = false): MqttWill =
  result = MqttWill(
    enabled: true,
    topic: topic,
    payload: @payload,
    qos: qos,
    retain: retain
  )

proc mqttWill*(topic: string; payload: string; qos = qos0;
               retain = false): MqttWill =
  result = mqttWill(topic, bytesFromString(payload), qos = qos, retain = retain)

proc userProperty*(name, value: string): MqttProperty =
  ## Construct an MQTT v5 User Property.
  ##
  ## User Property is represented as a UTF-8 string pair in MQTT v5.  Empty names
  ## are rejected when the property is converted to libmosquitto properties; this
  ## constructor remains allocation-only and does not raise.
  result = MqttProperty(kind: mpUserProperty, name: name, value: value)


proc responseTopic*(topic: string): MqttProperty =
  ## Construct an MQTT v5 Response Topic property.
  ##
  ## The topic name is validated when the property is converted to a libmosquitto
  ## property list.
  result = MqttProperty(kind: mpResponseTopic, value: topic)

proc correlationData*(data: openArray[byte]): MqttProperty =
  ## Construct an MQTT v5 Correlation Data property.
  result = MqttProperty(kind: mpCorrelationData, data: @data)

proc correlationData*(data: string): MqttProperty =
  ## Construct an MQTT v5 Correlation Data property from a string.
  result = correlationData(bytesFromString(data))

proc messageExpiryInterval*(seconds: uint32): MqttProperty =
  ## Construct an MQTT v5 Message Expiry Interval property.
  ##
  ## The value is seconds. A value of 0 means the message expires immediately
  ## after it has been processed by the server.
  result = MqttProperty(kind: mpMessageExpiryInterval, intValue: seconds)

proc contentType*(value: string): MqttProperty =
  ## Construct an MQTT v5 Content Type property.
  result = MqttProperty(kind: mpContentType, value: value)

proc payloadFormatIndicator*(indicator: MqttPayloadFormatIndicator): MqttProperty =
  ## Construct an MQTT v5 Payload Format Indicator property.
  result = MqttProperty(kind: mpPayloadFormatIndicator, intValue: ord(indicator).uint32)

proc payloadFormatIndicator*(value: uint8): MqttProperty =
  ## Construct an MQTT v5 Payload Format Indicator property from a raw byte.
  ##
  ## This overload is useful when copying values from libmosquitto. Validation is
  ## performed by buildMosquittoProperties() before sending.
  result = MqttProperty(kind: mpPayloadFormatIndicator, intValue: value.uint32)

proc payloadFormatIndicatorUtf8*(): MqttProperty =
  ## Construct Payload Format Indicator = UTF-8 encoded character data.
  result = payloadFormatIndicator(mpfiUtf8)

proc payloadFormatIndicatorUnspecified*(): MqttProperty =
  ## Construct Payload Format Indicator = unspecified/binary payload.
  result = payloadFormatIndicator(mpfiUnspecified)

# ------------------------------------------------------------------------------
# Typed MQTT v5 property containers
# ------------------------------------------------------------------------------
proc noConnectProperties*(): MqttConnectProperties =
  ## Construct an empty typed MQTT v5 CONNECT property set.
  result = MqttConnectProperties()

proc noPublishProperties*(): MqttPublishProperties =
  ## Construct an empty typed MQTT v5 PUBLISH property set.
  result = MqttPublishProperties()

proc noSubscribeProperties*(): MqttSubscribeProperties =
  ## Construct an empty typed MQTT v5 SUBSCRIBE property set.
  result = MqttSubscribeProperties()

proc toMqttProperties*(properties: MqttPublishProperties): MqttProperties =
  ## Convert typed PUBLISH properties to the generic lowlevel representation.
  ##
  ## This conversion is allocation-only. Topic/string/value validation still
  ## happens in buildMosquittoProperties(), where libmosquitto property lists are
  ## actually created.
  result = @[]

  for item in properties.userProperties:
    result.add(userProperty(item[0], item[1]))

  if properties.responseTopic.isSome:
    result.add(responseTopic(properties.responseTopic.get()))

  if properties.correlationData.isSome:
    result.add(correlationData(properties.correlationData.get()))

  if properties.messageExpiryInterval.isSome:
    result.add(messageExpiryInterval(properties.messageExpiryInterval.get()))

  if properties.contentType.isSome:
    result.add(contentType(properties.contentType.get()))

  if properties.payloadFormatIndicator.isSome:
    result.add(payloadFormatIndicator(properties.payloadFormatIndicator.get()))

proc buildMqttPublishProperties(properties: openArray[MqttProperty]): MqttResult[MqttPublishProperties] =
  var typed = noPublishProperties()

  for property in properties:
    case property.kind
    of mpUserProperty:
      typed.userProperties.add((property.name, property.value))
    of mpResponseTopic:
      if typed.responseTopic.isSome:
        return err(invalidArgument("MQTT publish properties", "Response Topic appears more than once"))
      typed.responseTopic = some(property.value)
    of mpCorrelationData:
      if typed.correlationData.isSome:
        return err(invalidArgument("MQTT publish properties", "Correlation Data appears more than once"))
      typed.correlationData = some(property.data)
    of mpMessageExpiryInterval:
      if typed.messageExpiryInterval.isSome:
        return err(invalidArgument("MQTT publish properties", "Message Expiry Interval appears more than once"))
      typed.messageExpiryInterval = some(property.intValue)
    of mpContentType:
      if typed.contentType.isSome:
        return err(invalidArgument("MQTT publish properties", "Content Type appears more than once"))
      typed.contentType = some(property.value)
    of mpPayloadFormatIndicator:
      if typed.payloadFormatIndicator.isSome:
        return err(invalidArgument("MQTT publish properties", "Payload Format Indicator appears more than once"))
      let indicatorRes = toMqttPayloadFormatIndicator(property.intValue)
      if indicatorRes.isErr:
        return err(indicatorRes.error)
      typed.payloadFormatIndicator = some(indicatorRes.get())
    of mpUnknown:
      return err(invalidArgument("MQTT publish properties", "unknown property is not valid for PUBLISH"))

  result = ok(typed)

proc mqttPublishProperties*(properties: openArray[MqttProperty]): MqttResult[MqttPublishProperties] =
  ## Convert generic MQTT v5 properties into a typed PUBLISH property set.
  ##
  ## User Property may appear multiple times. The other currently supported
  ## PUBLISH properties are single-instance and are rejected if duplicated.
  result = buildMqttPublishProperties(properties)

proc mqttPublishProperties*(properties: varargs[MqttProperty]): MqttResult[MqttPublishProperties] =
  ## Varargs convenience overload for typed MQTT v5 PUBLISH properties.
  result = buildMqttPublishProperties(properties)

proc addUserProperty*(properties: var MqttPublishProperties; name, value: string) =
  ## Add a User Property to a typed PUBLISH property set.
  properties.userProperties.add((name, value))

proc setResponseTopic*(properties: var MqttPublishProperties; topic: string) =
  ## Set the Response Topic in a typed PUBLISH property set.
  properties.responseTopic = some(topic)

proc setCorrelationData*(properties: var MqttPublishProperties; data: openArray[byte]) =
  ## Set binary Correlation Data in a typed PUBLISH property set.
  properties.correlationData = some(@data)

proc setCorrelationData*(properties: var MqttPublishProperties; data: string) =
  ## Set textual Correlation Data in a typed PUBLISH property set.
  properties.correlationData = some(bytesFromString(data))

proc setMessageExpiryInterval*(properties: var MqttPublishProperties; seconds: uint32) =
  ## Set Message Expiry Interval in a typed PUBLISH property set.
  properties.messageExpiryInterval = some(seconds)

proc setContentType*(properties: var MqttPublishProperties; value: string) =
  ## Set Content Type in a typed PUBLISH property set.
  properties.contentType = some(value)

proc setPayloadFormatIndicator*(properties: var MqttPublishProperties; indicator: MqttPayloadFormatIndicator) =
  ## Set Payload Format Indicator in a typed PUBLISH property set.
  properties.payloadFormatIndicator = some(indicator)

proc hasProperties*(properties: MqttProperties): bool {.inline.} =
  result = properties.len > 0

proc propertyDataString*(property: MqttProperty): string =
  ## Return binary property data as a Nim string.
  ##
  ## This is a convenience helper for textual Correlation Data. Binary users
  ## should use `property.data` directly.
  if property.data.len == 0:
    return ""

  result = newString(property.data.len)
  copyMem(addr result[0], unsafeAddr property.data[0], property.data.len)

proc payloadString*(message: MqttMessage): string =
  ## Return payload bytes as a Nim string.
  ##
  ## This is a convenience helper for text payloads and nmqtt-compatible APIs.
  ## Binary payloads should use `message.payload` directly.
  if message.payload.len == 0:
    return ""

  result = newString(message.payload.len)
  copyMem(addr result[0], unsafeAddr message.payload[0], message.payload.len)
