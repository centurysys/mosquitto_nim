# Destination: src/mosquitto_nim/lowlevel/library.nim

import ./bindings/c_api
import ./errors
import ./types

# ------------------------------------------------------------------------------
# Process-wide libmosquitto lifecycle helpers.
#
# Keep these thin at this layer. Higher layers can decide whether to call them
# once globally, or wrap them with a once/guard mechanism.
# ------------------------------------------------------------------------------
proc initLibrary*(): MqttResult[MqttOk] =
  result = checkMosq(mosquitto_lib_init(), "mosquitto_lib_init")

proc cleanupLibrary*(): MqttResult[MqttOk] =
  result = checkMosq(mosquitto_lib_cleanup(), "mosquitto_lib_cleanup")

proc libVersion*(): MqttVersion =
  var major, minor, revision: cint
  discard mosquitto_lib_version(addr major, addr minor, addr revision)

  result = MqttVersion(
    major: major.int,
    minor: minor.int,
    revision: revision.int
  )

proc libVersionString*(): string =
  result = $libVersion()
