
type
  enum_mosq_err_t* {.size: sizeof(cint).} = enum
    MOSQ_ERR_AUTH_CONTINUE = -4, MOSQ_ERR_NO_SUBSCRIBERS = -3,
    MOSQ_ERR_SUB_EXISTS = -2, MOSQ_ERR_CONN_PENDING = -1, MOSQ_ERR_SUCCESS = 0,
    MOSQ_ERR_NOMEM = 1, MOSQ_ERR_PROTOCOL = 2, MOSQ_ERR_INVAL = 3,
    MOSQ_ERR_NO_CONN = 4, MOSQ_ERR_CONN_REFUSED = 5, MOSQ_ERR_NOT_FOUND = 6,
    MOSQ_ERR_CONN_LOST = 7, MOSQ_ERR_TLS = 8, MOSQ_ERR_PAYLOAD_SIZE = 9,
    MOSQ_ERR_NOT_SUPPORTED = 10, MOSQ_ERR_AUTH = 11, MOSQ_ERR_ACL_DENIED = 12,
    MOSQ_ERR_UNKNOWN = 13, MOSQ_ERR_ERRNO = 14, MOSQ_ERR_EAI = 15,
    MOSQ_ERR_PROXY = 16, MOSQ_ERR_PLUGIN_DEFER = 17,
    MOSQ_ERR_MALFORMED_UTF8 = 18, MOSQ_ERR_KEEPALIVE = 19, MOSQ_ERR_LOOKUP = 20,
    MOSQ_ERR_MALFORMED_PACKET = 21, MOSQ_ERR_DUPLICATE_PROPERTY = 22,
    MOSQ_ERR_TLS_HANDSHAKE = 23, MOSQ_ERR_QOS_NOT_SUPPORTED = 24,
    MOSQ_ERR_OVERSIZE_PACKET = 25, MOSQ_ERR_OCSP = 26, MOSQ_ERR_TIMEOUT = 27,
    MOSQ_ERR_RETAIN_NOT_SUPPORTED = 28, MOSQ_ERR_TOPIC_ALIAS_INVALID = 29,
    MOSQ_ERR_ADMINISTRATIVE_ACTION = 30, MOSQ_ERR_ALREADY_EXISTS = 31
type
  enum_mosq_opt_t* {.size: sizeof(cuint).} = enum
    MOSQ_OPT_PROTOCOL_VERSION = 1, MOSQ_OPT_SSL_CTX = 2,
    MOSQ_OPT_SSL_CTX_WITH_DEFAULTS = 3, MOSQ_OPT_RECEIVE_MAXIMUM = 4,
    MOSQ_OPT_SEND_MAXIMUM = 5, MOSQ_OPT_TLS_KEYFORM = 6,
    MOSQ_OPT_TLS_ENGINE = 7, MOSQ_OPT_TLS_ENGINE_KPASS_SHA1 = 8,
    MOSQ_OPT_TLS_OCSP_REQUIRED = 9, MOSQ_OPT_TLS_ALPN = 10,
    MOSQ_OPT_TCP_NODELAY = 11, MOSQ_OPT_BIND_ADDRESS = 12,
    MOSQ_OPT_TLS_USE_OS_CERTS = 13
type
  struct_mqtt5_property* = object
type
  struct_mosquitto* = object
type
  struct_mosquitto_message* {.pure, inheritable, bycopy.} = object
    mid*: cint               ## Generated based on /usr/include/mosquitto.h:175:8
    topic*: cstring
    payload*: pointer
    payloadlen*: cint
    qos*: cint
    retain*: bool
  mosquitto_property* = struct_mqtt5_property ## Generated based on /usr/include/mosquitto.h:185:32
  struct_libmosquitto_will* {.pure, inheritable, bycopy.} = object
    topic*: cstring          ## Generated based on /usr/include/mosquitto.h:2601:8
    payload*: pointer
    payloadlen*: cint
    qos*: cint
    retain*: bool
  struct_libmosquitto_auth* {.pure, inheritable, bycopy.} = object
    username*: cstring       ## Generated based on /usr/include/mosquitto.h:2609:8
    password*: cstring
  struct_libmosquitto_tls* {.pure, inheritable, bycopy.} = object
    cafile*: cstring         ## Generated based on /usr/include/mosquitto.h:2614:8
    capath*: cstring
    certfile*: cstring
    keyfile*: cstring
    ciphers*: cstring
    tls_version*: cstring
    pw_callback*: proc (a0: cstring; a1: cint; a2: cint; a3: pointer): cint {.
        cdecl.}
    cert_reqs*: cint
when 2 is static:
  const
    LIBMOSQUITTO_MAJOR* = 2  ## Generated based on /usr/include/mosquitto.h:67:9
else:
  let LIBMOSQUITTO_MAJOR* = 2 ## Generated based on /usr/include/mosquitto.h:67:9
when 0 is static:
  const
    LIBMOSQUITTO_MINOR* = 0  ## Generated based on /usr/include/mosquitto.h:68:9
else:
  let LIBMOSQUITTO_MINOR* = 0 ## Generated based on /usr/include/mosquitto.h:68:9
when 18 is static:
  const
    LIBMOSQUITTO_REVISION* = 18 ## Generated based on /usr/include/mosquitto.h:69:9
else:
  let LIBMOSQUITTO_REVISION* = 18 ## Generated based on /usr/include/mosquitto.h:69:9
when 0 is static:
  const
    MOSQ_LOG_NONE* = 0       ## Generated based on /usr/include/mosquitto.h:74:9
else:
  let MOSQ_LOG_NONE* = 0     ## Generated based on /usr/include/mosquitto.h:74:9
when cast[cuint](2147483648'i64) is static:
  const
    MOSQ_LOG_INTERNAL* = cast[cuint](2147483648'i64) ## Generated based on /usr/include/mosquitto.h:83:9
else:
  let MOSQ_LOG_INTERNAL* = cast[cuint](2147483648'i64) ## Generated based on /usr/include/mosquitto.h:83:9
when cast[cuint](4294967295'i64) is static:
  const
    MOSQ_LOG_ALL* = cast[cuint](4294967295'i64) ## Generated based on /usr/include/mosquitto.h:84:9
else:
  let MOSQ_LOG_ALL* = cast[cuint](4294967295'i64) ## Generated based on /usr/include/mosquitto.h:84:9
when 23 is static:
  const
    MOSQ_MQTT_ID_MAX_LENGTH* = 23 ## Generated based on /usr/include/mosquitto.h:151:9
else:
  let MOSQ_MQTT_ID_MAX_LENGTH* = 23 ## Generated based on /usr/include/mosquitto.h:151:9
when 3 is static:
  const
    MQTT_PROTOCOL_V31* = 3   ## Generated based on /usr/include/mosquitto.h:153:9
else:
  let MQTT_PROTOCOL_V31* = 3 ## Generated based on /usr/include/mosquitto.h:153:9
when 4 is static:
  const
    MQTT_PROTOCOL_V311* = 4  ## Generated based on /usr/include/mosquitto.h:154:9
else:
  let MQTT_PROTOCOL_V311* = 4 ## Generated based on /usr/include/mosquitto.h:154:9
when 5 is static:
  const
    MQTT_PROTOCOL_V5* = 5    ## Generated based on /usr/include/mosquitto.h:155:9
else:
  let MQTT_PROTOCOL_V5* = 5  ## Generated based on /usr/include/mosquitto.h:155:9
proc mosquitto_lib_version*(major: ptr cint; minor: ptr cint; revision: ptr cint): cint {.
    cdecl, importc: "mosquitto_lib_version".}
proc mosquitto_lib_init*(): cint {.cdecl, importc: "mosquitto_lib_init".}
proc mosquitto_lib_cleanup*(): cint {.cdecl, importc: "mosquitto_lib_cleanup".}
proc mosquitto_new*(id: cstring; clean_session: bool; obj: pointer): ptr struct_mosquitto {.
    cdecl, importc: "mosquitto_new".}
proc mosquitto_destroy*(mosq: ptr struct_mosquitto): void {.cdecl,
    importc: "mosquitto_destroy".}
proc mosquitto_reinitialise*(mosq: ptr struct_mosquitto; id: cstring;
                             clean_session: bool; obj: pointer): cint {.cdecl,
    importc: "mosquitto_reinitialise".}
proc mosquitto_will_set*(mosq: ptr struct_mosquitto; topic: cstring;
                         payloadlen: cint; payload: pointer; qos: cint;
                         retain: bool): cint {.cdecl,
    importc: "mosquitto_will_set".}
proc mosquitto_will_set_v5*(mosq: ptr struct_mosquitto; topic: cstring;
                            payloadlen: cint; payload: pointer; qos: cint;
                            retain: bool; properties: ptr mosquitto_property): cint {.
    cdecl, importc: "mosquitto_will_set_v5".}
proc mosquitto_will_clear*(mosq: ptr struct_mosquitto): cint {.cdecl,
    importc: "mosquitto_will_clear".}
proc mosquitto_username_pw_set*(mosq: ptr struct_mosquitto; username: cstring;
                                password: cstring): cint {.cdecl,
    importc: "mosquitto_username_pw_set".}
proc mosquitto_connect*(mosq: ptr struct_mosquitto; host: cstring; port: cint;
                        keepalive: cint): cint {.cdecl,
    importc: "mosquitto_connect".}
proc mosquitto_connect_bind*(mosq: ptr struct_mosquitto; host: cstring;
                             port: cint; keepalive: cint; bind_address: cstring): cint {.
    cdecl, importc: "mosquitto_connect_bind".}
proc mosquitto_connect_bind_v5*(mosq: ptr struct_mosquitto; host: cstring;
                                port: cint; keepalive: cint;
                                bind_address: cstring;
                                properties: ptr mosquitto_property): cint {.
    cdecl, importc: "mosquitto_connect_bind_v5".}
proc mosquitto_connect_async*(mosq: ptr struct_mosquitto; host: cstring;
                              port: cint; keepalive: cint): cint {.cdecl,
    importc: "mosquitto_connect_async".}
proc mosquitto_connect_bind_async*(mosq: ptr struct_mosquitto; host: cstring;
                                   port: cint; keepalive: cint;
                                   bind_address: cstring): cint {.cdecl,
    importc: "mosquitto_connect_bind_async".}
proc mosquitto_connect_srv*(mosq: ptr struct_mosquitto; host: cstring;
                            keepalive: cint; bind_address: cstring): cint {.
    cdecl, importc: "mosquitto_connect_srv".}
proc mosquitto_reconnect*(mosq: ptr struct_mosquitto): cint {.cdecl,
    importc: "mosquitto_reconnect".}
proc mosquitto_reconnect_async*(mosq: ptr struct_mosquitto): cint {.cdecl,
    importc: "mosquitto_reconnect_async".}
proc mosquitto_disconnect*(mosq: ptr struct_mosquitto): cint {.cdecl,
    importc: "mosquitto_disconnect".}
proc mosquitto_disconnect_v5*(mosq: ptr struct_mosquitto; reason_code: cint;
                              properties: ptr mosquitto_property): cint {.cdecl,
    importc: "mosquitto_disconnect_v5".}
proc mosquitto_publish*(mosq: ptr struct_mosquitto; mid: ptr cint;
                        topic: cstring; payloadlen: cint; payload: pointer;
                        qos: cint; retain: bool): cint {.cdecl,
    importc: "mosquitto_publish".}
proc mosquitto_publish_v5*(mosq: ptr struct_mosquitto; mid: ptr cint;
                           topic: cstring; payloadlen: cint; payload: pointer;
                           qos: cint; retain: bool;
                           properties: ptr mosquitto_property): cint {.cdecl,
    importc: "mosquitto_publish_v5".}
proc mosquitto_subscribe*(mosq: ptr struct_mosquitto; mid: ptr cint;
                          sub: cstring; qos: cint): cint {.cdecl,
    importc: "mosquitto_subscribe".}
proc mosquitto_subscribe_v5*(mosq: ptr struct_mosquitto; mid: ptr cint;
                             sub: cstring; qos: cint; options: cint;
                             properties: ptr mosquitto_property): cint {.cdecl,
    importc: "mosquitto_subscribe_v5".}
proc mosquitto_subscribe_multiple*(mosq: ptr struct_mosquitto; mid: ptr cint;
                                   sub_count: cint; sub: ptr cstring; qos: cint;
                                   options: cint;
                                   properties: ptr mosquitto_property): cint {.
    cdecl, importc: "mosquitto_subscribe_multiple".}
proc mosquitto_unsubscribe*(mosq: ptr struct_mosquitto; mid: ptr cint;
                            sub: cstring): cint {.cdecl,
    importc: "mosquitto_unsubscribe".}
proc mosquitto_unsubscribe_v5*(mosq: ptr struct_mosquitto; mid: ptr cint;
                               sub: cstring; properties: ptr mosquitto_property): cint {.
    cdecl, importc: "mosquitto_unsubscribe_v5".}
proc mosquitto_unsubscribe_multiple*(mosq: ptr struct_mosquitto; mid: ptr cint;
                                     sub_count: cint; sub: ptr cstring;
                                     properties: ptr mosquitto_property): cint {.
    cdecl, importc: "mosquitto_unsubscribe_multiple".}
proc mosquitto_message_copy*(dst: ptr struct_mosquitto_message;
                             src: ptr struct_mosquitto_message): cint {.cdecl,
    importc: "mosquitto_message_copy".}
proc mosquitto_message_free*(message: ptr ptr struct_mosquitto_message): void {.
    cdecl, importc: "mosquitto_message_free".}
proc mosquitto_message_free_contents*(message: ptr struct_mosquitto_message): void {.
    cdecl, importc: "mosquitto_message_free_contents".}
proc mosquitto_loop_forever*(mosq: ptr struct_mosquitto; timeout: cint;
                             max_packets: cint): cint {.cdecl,
    importc: "mosquitto_loop_forever".}
proc mosquitto_loop_start*(mosq: ptr struct_mosquitto): cint {.cdecl,
    importc: "mosquitto_loop_start".}
proc mosquitto_loop_stop*(mosq: ptr struct_mosquitto; force: bool): cint {.
    cdecl, importc: "mosquitto_loop_stop".}
proc mosquitto_loop*(mosq: ptr struct_mosquitto; timeout: cint;
                     max_packets: cint): cint {.cdecl, importc: "mosquitto_loop".}
proc mosquitto_loop_read*(mosq: ptr struct_mosquitto; max_packets: cint): cint {.
    cdecl, importc: "mosquitto_loop_read".}
proc mosquitto_loop_write*(mosq: ptr struct_mosquitto; max_packets: cint): cint {.
    cdecl, importc: "mosquitto_loop_write".}
proc mosquitto_loop_misc*(mosq: ptr struct_mosquitto): cint {.cdecl,
    importc: "mosquitto_loop_misc".}
proc mosquitto_socket*(mosq: ptr struct_mosquitto): cint {.cdecl,
    importc: "mosquitto_socket".}
proc mosquitto_want_write*(mosq: ptr struct_mosquitto): bool {.cdecl,
    importc: "mosquitto_want_write".}
proc mosquitto_threaded_set*(mosq: ptr struct_mosquitto; threaded: bool): cint {.
    cdecl, importc: "mosquitto_threaded_set".}
proc mosquitto_opts_set*(mosq: ptr struct_mosquitto; option: enum_mosq_opt_t;
                         value: pointer): cint {.cdecl,
    importc: "mosquitto_opts_set".}
proc mosquitto_int_option*(mosq: ptr struct_mosquitto; option: enum_mosq_opt_t;
                           value: cint): cint {.cdecl,
    importc: "mosquitto_int_option".}
proc mosquitto_string_option*(mosq: ptr struct_mosquitto;
                              option: enum_mosq_opt_t; value: cstring): cint {.
    cdecl, importc: "mosquitto_string_option".}
proc mosquitto_void_option*(mosq: ptr struct_mosquitto; option: enum_mosq_opt_t;
                            value: pointer): cint {.cdecl,
    importc: "mosquitto_void_option".}
proc mosquitto_reconnect_delay_set*(mosq: ptr struct_mosquitto;
                                    reconnect_delay: cuint;
                                    reconnect_delay_max: cuint;
                                    reconnect_exponential_backoff: bool): cint {.
    cdecl, importc: "mosquitto_reconnect_delay_set".}
proc mosquitto_max_inflight_messages_set*(mosq: ptr struct_mosquitto;
    max_inflight_messages: cuint): cint {.cdecl,
    importc: "mosquitto_max_inflight_messages_set".}
proc mosquitto_message_retry_set*(mosq: ptr struct_mosquitto;
                                  message_retry: cuint): void {.cdecl,
    importc: "mosquitto_message_retry_set".}
proc mosquitto_user_data_set*(mosq: ptr struct_mosquitto; obj: pointer): void {.
    cdecl, importc: "mosquitto_user_data_set".}
proc mosquitto_userdata*(mosq: ptr struct_mosquitto): pointer {.cdecl,
    importc: "mosquitto_userdata".}
proc mosquitto_tls_set*(mosq: ptr struct_mosquitto; cafile: cstring;
                        capath: cstring; certfile: cstring; keyfile: cstring;
    pw_callback: proc (a0: cstring; a1: cint; a2: cint; a3: pointer): cint {.
    cdecl.}): cint {.cdecl, importc: "mosquitto_tls_set".}
proc mosquitto_tls_insecure_set*(mosq: ptr struct_mosquitto; value: bool): cint {.
    cdecl, importc: "mosquitto_tls_insecure_set".}
proc mosquitto_tls_opts_set*(mosq: ptr struct_mosquitto; cert_reqs: cint;
                             tls_version: cstring; ciphers: cstring): cint {.
    cdecl, importc: "mosquitto_tls_opts_set".}
proc mosquitto_tls_psk_set*(mosq: ptr struct_mosquitto; psk: cstring;
                            identity: cstring; ciphers: cstring): cint {.cdecl,
    importc: "mosquitto_tls_psk_set".}
proc mosquitto_ssl_get*(mosq: ptr struct_mosquitto): pointer {.cdecl,
    importc: "mosquitto_ssl_get".}
proc mosquitto_connect_callback_set*(mosq: ptr struct_mosquitto; on_connect: proc (
    a0: ptr struct_mosquitto; a1: pointer; a2: cint): void {.cdecl.}): void {.
    cdecl, importc: "mosquitto_connect_callback_set".}
proc mosquitto_connect_with_flags_callback_set*(mosq: ptr struct_mosquitto;
    on_connect: proc (a0: ptr struct_mosquitto; a1: pointer; a2: cint; a3: cint): void {.
    cdecl.}): void {.cdecl, importc: "mosquitto_connect_with_flags_callback_set".}
proc mosquitto_connect_v5_callback_set*(mosq: ptr struct_mosquitto; on_connect: proc (
    a0: ptr struct_mosquitto; a1: pointer; a2: cint; a3: cint;
    a4: ptr mosquitto_property): void {.cdecl.}): void {.cdecl,
    importc: "mosquitto_connect_v5_callback_set".}
proc mosquitto_disconnect_callback_set*(mosq: ptr struct_mosquitto;
    on_disconnect: proc (a0: ptr struct_mosquitto; a1: pointer; a2: cint): void {.
    cdecl.}): void {.cdecl, importc: "mosquitto_disconnect_callback_set".}
proc mosquitto_disconnect_v5_callback_set*(mosq: ptr struct_mosquitto;
    on_disconnect: proc (a0: ptr struct_mosquitto; a1: pointer; a2: cint;
                         a3: ptr mosquitto_property): void {.cdecl.}): void {.
    cdecl, importc: "mosquitto_disconnect_v5_callback_set".}
proc mosquitto_publish_callback_set*(mosq: ptr struct_mosquitto; on_publish: proc (
    a0: ptr struct_mosquitto; a1: pointer; a2: cint): void {.cdecl.}): void {.
    cdecl, importc: "mosquitto_publish_callback_set".}
proc mosquitto_publish_v5_callback_set*(mosq: ptr struct_mosquitto; on_publish: proc (
    a0: ptr struct_mosquitto; a1: pointer; a2: cint; a3: cint;
    a4: ptr mosquitto_property): void {.cdecl.}): void {.cdecl,
    importc: "mosquitto_publish_v5_callback_set".}
proc mosquitto_message_callback_set*(mosq: ptr struct_mosquitto; on_message: proc (
    a0: ptr struct_mosquitto; a1: pointer; a2: ptr struct_mosquitto_message): void {.
    cdecl.}): void {.cdecl, importc: "mosquitto_message_callback_set".}
proc mosquitto_message_v5_callback_set*(mosq: ptr struct_mosquitto; on_message: proc (
    a0: ptr struct_mosquitto; a1: pointer; a2: ptr struct_mosquitto_message;
    a3: ptr mosquitto_property): void {.cdecl.}): void {.cdecl,
    importc: "mosquitto_message_v5_callback_set".}
proc mosquitto_subscribe_callback_set*(mosq: ptr struct_mosquitto; on_subscribe: proc (
    a0: ptr struct_mosquitto; a1: pointer; a2: cint; a3: cint; a4: ptr cint): void {.
    cdecl.}): void {.cdecl, importc: "mosquitto_subscribe_callback_set".}
proc mosquitto_subscribe_v5_callback_set*(mosq: ptr struct_mosquitto;
    on_subscribe: proc (a0: ptr struct_mosquitto; a1: pointer; a2: cint;
                        a3: cint; a4: ptr cint; a5: ptr mosquitto_property): void {.
    cdecl.}): void {.cdecl, importc: "mosquitto_subscribe_v5_callback_set".}
proc mosquitto_unsubscribe_callback_set*(mosq: ptr struct_mosquitto;
    on_unsubscribe: proc (a0: ptr struct_mosquitto; a1: pointer; a2: cint): void {.
    cdecl.}): void {.cdecl, importc: "mosquitto_unsubscribe_callback_set".}
proc mosquitto_unsubscribe_v5_callback_set*(mosq: ptr struct_mosquitto;
    on_unsubscribe: proc (a0: ptr struct_mosquitto; a1: pointer; a2: cint;
                          a3: ptr mosquitto_property): void {.cdecl.}): void {.
    cdecl, importc: "mosquitto_unsubscribe_v5_callback_set".}
proc mosquitto_log_callback_set*(mosq: ptr struct_mosquitto; on_log: proc (
    a0: ptr struct_mosquitto; a1: pointer; a2: cint; a3: cstring): void {.cdecl.}): void {.
    cdecl, importc: "mosquitto_log_callback_set".}
proc mosquitto_socks5_set*(mosq: ptr struct_mosquitto; host: cstring;
                           port: cint; username: cstring; password: cstring): cint {.
    cdecl, importc: "mosquitto_socks5_set".}
proc mosquitto_strerror*(mosq_errno: cint): cstring {.cdecl,
    importc: "mosquitto_strerror".}
proc mosquitto_connack_string*(connack_code: cint): cstring {.cdecl,
    importc: "mosquitto_connack_string".}
proc mosquitto_reason_string*(reason_code: cint): cstring {.cdecl,
    importc: "mosquitto_reason_string".}
proc mosquitto_string_to_command*(str: cstring; cmd: ptr cint): cint {.cdecl,
    importc: "mosquitto_string_to_command".}
proc mosquitto_sub_topic_tokenise*(subtopic: cstring; topics: ptr ptr cstring;
                                   count: ptr cint): cint {.cdecl,
    importc: "mosquitto_sub_topic_tokenise".}
proc mosquitto_sub_topic_tokens_free*(topics: ptr ptr cstring; count: cint): cint {.
    cdecl, importc: "mosquitto_sub_topic_tokens_free".}
proc mosquitto_topic_matches_sub*(sub: cstring; topic: cstring; result: ptr bool): cint {.
    cdecl, importc: "mosquitto_topic_matches_sub".}
proc mosquitto_topic_matches_sub2*(sub: cstring; sublen: csize_t;
                                   topic: cstring; topiclen: csize_t;
                                   result: ptr bool): cint {.cdecl,
    importc: "mosquitto_topic_matches_sub2".}
proc mosquitto_pub_topic_check*(topic: cstring): cint {.cdecl,
    importc: "mosquitto_pub_topic_check".}
proc mosquitto_pub_topic_check2*(topic: cstring; topiclen: csize_t): cint {.
    cdecl, importc: "mosquitto_pub_topic_check2".}
proc mosquitto_sub_topic_check*(topic: cstring): cint {.cdecl,
    importc: "mosquitto_sub_topic_check".}
proc mosquitto_sub_topic_check2*(topic: cstring; topiclen: csize_t): cint {.
    cdecl, importc: "mosquitto_sub_topic_check2".}
proc mosquitto_validate_utf8*(str: cstring; len: cint): cint {.cdecl,
    importc: "mosquitto_validate_utf8".}
proc mosquitto_subscribe_simple*(messages: ptr ptr struct_mosquitto_message;
                                 msg_count: cint; want_retained: bool;
                                 topic: cstring; qos: cint; host: cstring;
                                 port: cint; client_id: cstring;
                                 keepalive: cint; clean_session: bool;
                                 username: cstring; password: cstring;
                                 will: ptr struct_libmosquitto_will;
                                 tls: ptr struct_libmosquitto_tls): cint {.
    cdecl, importc: "mosquitto_subscribe_simple".}
proc mosquitto_subscribe_callback*(callback: proc (a0: ptr struct_mosquitto;
    a1: pointer; a2: ptr struct_mosquitto_message): cint {.cdecl.};
                                   userdata: pointer; topic: cstring; qos: cint;
                                   host: cstring; port: cint;
                                   client_id: cstring; keepalive: cint;
                                   clean_session: bool; username: cstring;
                                   password: cstring;
                                   will: ptr struct_libmosquitto_will;
                                   tls: ptr struct_libmosquitto_tls): cint {.
    cdecl, importc: "mosquitto_subscribe_callback".}
proc mosquitto_property_add_byte*(proplist: ptr ptr mosquitto_property;
                                  identifier: cint; value: uint8): cint {.cdecl,
    importc: "mosquitto_property_add_byte".}
proc mosquitto_property_add_int16*(proplist: ptr ptr mosquitto_property;
                                   identifier: cint; value: uint16): cint {.
    cdecl, importc: "mosquitto_property_add_int16".}
proc mosquitto_property_add_int32*(proplist: ptr ptr mosquitto_property;
                                   identifier: cint; value: uint32): cint {.
    cdecl, importc: "mosquitto_property_add_int32".}
proc mosquitto_property_add_varint*(proplist: ptr ptr mosquitto_property;
                                    identifier: cint; value: uint32): cint {.
    cdecl, importc: "mosquitto_property_add_varint".}
proc mosquitto_property_add_binary*(proplist: ptr ptr mosquitto_property;
                                    identifier: cint; value: pointer;
                                    len: uint16): cint {.cdecl,
    importc: "mosquitto_property_add_binary".}
proc mosquitto_property_add_string*(proplist: ptr ptr mosquitto_property;
                                    identifier: cint; value: cstring): cint {.
    cdecl, importc: "mosquitto_property_add_string".}
proc mosquitto_property_add_string_pair*(proplist: ptr ptr mosquitto_property;
    identifier: cint; name: cstring; value: cstring): cint {.cdecl,
    importc: "mosquitto_property_add_string_pair".}
proc mosquitto_property_identifier*(property: ptr mosquitto_property): cint {.
    cdecl, importc: "mosquitto_property_identifier".}
proc mosquitto_property_next*(proplist: ptr mosquitto_property): ptr mosquitto_property {.
    cdecl, importc: "mosquitto_property_next".}
proc mosquitto_property_read_byte*(proplist: ptr mosquitto_property;
                                   identifier: cint; value: ptr uint8;
                                   skip_first: bool): ptr mosquitto_property {.
    cdecl, importc: "mosquitto_property_read_byte".}
proc mosquitto_property_read_int16*(proplist: ptr mosquitto_property;
                                    identifier: cint; value: ptr uint16;
                                    skip_first: bool): ptr mosquitto_property {.
    cdecl, importc: "mosquitto_property_read_int16".}
proc mosquitto_property_read_int32*(proplist: ptr mosquitto_property;
                                    identifier: cint; value: ptr uint32;
                                    skip_first: bool): ptr mosquitto_property {.
    cdecl, importc: "mosquitto_property_read_int32".}
proc mosquitto_property_read_varint*(proplist: ptr mosquitto_property;
                                     identifier: cint; value: ptr uint32;
                                     skip_first: bool): ptr mosquitto_property {.
    cdecl, importc: "mosquitto_property_read_varint".}
proc mosquitto_property_read_binary*(proplist: ptr mosquitto_property;
                                     identifier: cint; value: ptr pointer;
                                     len: ptr uint16; skip_first: bool): ptr mosquitto_property {.
    cdecl, importc: "mosquitto_property_read_binary".}
proc mosquitto_property_read_string*(proplist: ptr mosquitto_property;
                                     identifier: cint; value: ptr cstring;
                                     skip_first: bool): ptr mosquitto_property {.
    cdecl, importc: "mosquitto_property_read_string".}
proc mosquitto_property_read_string_pair*(proplist: ptr mosquitto_property;
    identifier: cint; name: ptr cstring; value: ptr cstring; skip_first: bool): ptr mosquitto_property {.
    cdecl, importc: "mosquitto_property_read_string_pair".}
proc mosquitto_property_free_all*(properties: ptr ptr mosquitto_property): void {.
    cdecl, importc: "mosquitto_property_free_all".}
proc mosquitto_property_copy_all*(dest: ptr ptr mosquitto_property;
                                  src: ptr mosquitto_property): cint {.cdecl,
    importc: "mosquitto_property_copy_all".}
proc mosquitto_property_check_command*(command: cint; identifier: cint): cint {.
    cdecl, importc: "mosquitto_property_check_command".}
proc mosquitto_property_check_all*(command: cint;
                                   properties: ptr mosquitto_property): cint {.
    cdecl, importc: "mosquitto_property_check_all".}
proc mosquitto_property_identifier_to_string*(identifier: cint): cstring {.
    cdecl, importc: "mosquitto_property_identifier_to_string".}
proc mosquitto_string_to_property_info*(propname: cstring; identifier: ptr cint;
                                        type_arg: ptr cint): cint {.cdecl,
    importc: "mosquitto_string_to_property_info".}