# Destination: tests/test1.nim

import std/unittest

import results

import mosquitto_nim

suite "mosquitto_nim lowlevel smoke test":
  test "libmosquitto version is available":
    let version = libVersion()
    check version.major >= 1
    check libVersionString().len > 0

  test "libmosquitto init and cleanup":
    let initRes = initLibrary()
    check initRes.isOk

    let cleanupRes = cleanupLibrary()
    check cleanupRes.isOk

  test "libmosquitto strerror is available":
    check mqttStrError(0).len > 0
