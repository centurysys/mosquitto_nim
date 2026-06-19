# Destination: src/mosquitto_nim/lowlevel/bindings/c_api.nim

const LibMosquittoDynLib* =
  when defined(windows):
    "mosquitto.dll"
  elif defined(macosx):
    "libmosquitto.dylib"
  else:
    "libmosquitto.so(|.1)"

{.push dynlib: LibMosquittoDynLib.}
include ./generated/libmosquitto_gen
{.pop.}
