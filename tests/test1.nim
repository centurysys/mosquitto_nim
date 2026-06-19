# Destination: tests/test1.nim

import std/unittest

import results

import mosquitto_nim
import mosquitto_nim/lowlevel/bridge
import mosquitto_nim/lowlevel/bindings/c_api

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

  test "lowlevel client can be created and destroyed":
    check initLibrary().isOk

    let clientRes = newLowLevelClient("mosquitto_nim_step2")
    check clientRes.isOk

    let client = clientRes.get()
    check not client.isClosed
    check closeLowLevelClient(client).isOk
    check client.isClosed

    check cleanupLibrary().isOk

  test "mosquitto message is copied into Nim-owned memory":
    var topic = "mosquitto_nim/test"
    var payload = @[byte(ord('h')), byte(ord('e')), byte(ord('l')), byte(ord('l')), byte(ord('o'))]
    var cmsg = struct_mosquitto_message(
      mid: 42.cint,
      topic: topic.cstring,
      payload: cast[pointer](addr payload[0]),
      payloadlen: payload.len.cint,
      qos: 1.cint,
      retain: true
    )

    let msgRes = copyMessage(addr cmsg)
    check msgRes.isOk

    let msg = msgRes.get()
    check msg.mid == 42
    check msg.topic == "mosquitto_nim/test"
    check msg.payloadString() == "hello"
    check msg.qos == qos1
    check msg.retain
    check not msg.dup
