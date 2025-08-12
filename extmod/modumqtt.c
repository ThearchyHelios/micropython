/*
 * This file is part of the MicroPython project, http://micropython.org/
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2025 MicroPython Contributors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <stdio.h>
#include <string.h>

#include "py/runtime.h"
#include "py/stream.h"
#include "py/mperrno.h"
#include "py/objstr.h"

#if MICROPY_PY_UMQTT

#define MQTT_PROTOCOL_LEVEL_3_1_1 (4)

typedef struct _mp_obj_mqtt_client_t {
    mp_obj_base_t base;
    mp_obj_t sock;
    mp_obj_t client_id;
    mp_obj_t user;
    mp_obj_t pswd;
    mp_obj_t server;
    uint16_t port;
    uint16_t keepalive;
    uint16_t pid;
    bool clean_session;
    mp_obj_t cb;
    mp_obj_t lw_topic;
    mp_obj_t lw_msg;
    uint8_t lw_qos;
    bool lw_retain;
} mp_obj_mqtt_client_t;

STATIC mp_obj_t mqtt_make_new(const mp_obj_type_t *type, size_t n_args, size_t n_kw, const mp_obj_t *args);
STATIC mp_obj_t mqtt_connect(size_t n_args, const mp_obj_t *args);
STATIC mp_obj_t mqtt_disconnect(mp_obj_t self_in);
STATIC mp_obj_t mqtt_ping(mp_obj_t self_in);
STATIC mp_obj_t mqtt_publish(size_t n_args, const mp_obj_t *args);
STATIC mp_obj_t mqtt_subscribe(mp_obj_t self_in, mp_obj_t topic_in, mp_obj_t qos_in);
STATIC mp_obj_t mqtt_set_callback(mp_obj_t self_in, mp_obj_t cb_in);
STATIC mp_obj_t mqtt_set_last_will(size_t n_args, const mp_obj_t *args);
STATIC mp_obj_t mqtt_check_msg(mp_obj_t self_in);
STATIC mp_obj_t mqtt_wait_msg(mp_obj_t self_in);

STATIC void mqtt_write_byte(mp_obj_mqtt_client_t *self, uint8_t b) {
    int errcode;
    mp_stream_write_exactly(self->sock, &b, 1, &errcode);
    if (errcode != 0) {
        mp_raise_OSError(errcode);
    }
}

STATIC void mqtt_write_bytes(mp_obj_mqtt_client_t *self, const uint8_t *buf, size_t len) {
    int errcode;
    mp_stream_write_exactly(self->sock, (void *)buf, len, &errcode);
    if (errcode != 0) {
        mp_raise_OSError(errcode);
    }
}

STATIC void mqtt_write_string(mp_obj_mqtt_client_t *self, mp_obj_t str) {
    size_t len;
    const char *s = mp_obj_str_get_data(str, &len);
    mqtt_write_byte(self, len >> 8);
    mqtt_write_byte(self, len & 0xFF);
    mqtt_write_bytes(self, (const uint8_t *)s, len);
}

STATIC uint8_t mqtt_read_byte(mp_obj_mqtt_client_t *self) {
    uint8_t b;
    int errcode;
    mp_stream_read_exactly(self->sock, &b, 1, &errcode);
    if (errcode != 0) {
        mp_raise_OSError(errcode);
    }
    return b;
}

STATIC void mqtt_read_bytes(mp_obj_mqtt_client_t *self, uint8_t *buf, size_t len) {
    int errcode;
    mp_stream_read_exactly(self->sock, buf, len, &errcode);
    if (errcode != 0) {
        mp_raise_OSError(errcode);
    }
}

STATIC mp_obj_t mqtt_make_new(const mp_obj_type_t *type, size_t n_args, size_t n_kw, const mp_obj_t *args) {
    mp_arg_check_num(n_args, n_kw, 1, 7, true);
    
    mp_obj_mqtt_client_t *self = m_new_obj(mp_obj_mqtt_client_t);
    self->base.type = type;
    self->client_id = args[0];
    
    if (n_args > 1) {
        self->server = args[1];
    } else {
        self->server = mp_obj_new_str("127.0.0.1", 9);
    }
    
    self->port = (n_args > 2) ? mp_obj_get_int(args[2]) : 1883;
    self->user = (n_args > 3) ? args[3] : mp_const_none;
    self->pswd = (n_args > 4) ? args[4] : mp_const_none;
    self->keepalive = (n_args > 5) ? mp_obj_get_int(args[5]) : 0;
    self->clean_session = (n_args > 6) ? mp_obj_is_true(args[6]) : true;
    
    self->pid = 0;
    self->cb = mp_const_none;
    self->sock = mp_const_none;
    self->lw_topic = mp_const_none;
    self->lw_msg = mp_const_none;
    self->lw_qos = 0;
    self->lw_retain = false;
    
    return MP_OBJ_FROM_PTR(self);
}

STATIC mp_obj_t mqtt_connect(size_t n_args, const mp_obj_t *args) {
    mp_obj_mqtt_client_t *self = MP_OBJ_TO_PTR(args[0]);
    bool clean = (n_args > 1) ? mp_obj_is_true(args[1]) : self->clean_session;
    
    mp_obj_t socket_module = mp_import_name(qstr_from_str("usocket"), mp_const_none, MP_OBJ_NEW_SMALL_INT(0));
    mp_obj_t socket_func = mp_load_attr(socket_module, qstr_from_str("socket"));
    self->sock = mp_call_function_0(socket_func);
    
    mp_obj_t connect_func = mp_load_attr(self->sock, qstr_from_str("connect"));
    mp_obj_t addr[2] = { self->server, MP_OBJ_NEW_SMALL_INT(self->port) };
    mp_obj_t addr_tuple = mp_obj_new_tuple(2, addr);
    mp_call_function_1(connect_func, addr_tuple);
    
    size_t client_id_len;
    mp_obj_str_get_data(self->client_id, &client_id_len);
    
    uint8_t connect_flags = 0;
    if (clean) {
        connect_flags |= 0x02;
    }
    
    size_t payload_len = 2 + client_id_len;
    
    if (self->lw_topic != mp_const_none) {
        connect_flags |= 0x04;
        connect_flags |= (self->lw_qos & 0x03) << 3;
        if (self->lw_retain) {
            connect_flags |= 0x20;
        }
        size_t topic_len, msg_len;
        mp_obj_str_get_data(self->lw_topic, &topic_len);
        mp_obj_str_get_data(self->lw_msg, &msg_len);
        payload_len += 2 + topic_len + 2 + msg_len;
    }
    
    if (self->user != mp_const_none) {
        connect_flags |= 0x80;
        size_t user_len;
        mp_obj_str_get_data(self->user, &user_len);
        payload_len += 2 + user_len;
        
        if (self->pswd != mp_const_none) {
            connect_flags |= 0x40;
            size_t pswd_len;
            mp_obj_str_get_data(self->pswd, &pswd_len);
            payload_len += 2 + pswd_len;
        }
    }
    
    uint8_t fixed_header[] = {
        0x10,
        10 + payload_len,
        0x00, 0x04, 'M', 'Q', 'T', 'T',
        MQTT_PROTOCOL_LEVEL_3_1_1,
        connect_flags,
        (uint8_t)(self->keepalive >> 8),
        (uint8_t)(self->keepalive & 0xFF)
    };
    
    mqtt_write_bytes(self, fixed_header, sizeof(fixed_header));
    mqtt_write_string(self, self->client_id);
    
    if (self->lw_topic != mp_const_none) {
        mqtt_write_string(self, self->lw_topic);
        mqtt_write_string(self, self->lw_msg);
    }
    
    if (self->user != mp_const_none) {
        mqtt_write_string(self, self->user);
        if (self->pswd != mp_const_none) {
            mqtt_write_string(self, self->pswd);
        }
    }
    
    uint8_t resp[4];
    mqtt_read_bytes(self, resp, 4);
    
    if (resp[0] != 0x20 || resp[1] != 0x02) {
        mp_raise_msg(&mp_type_OSError, MP_ERROR_TEXT("Invalid CONNACK"));
    }
    
    if (resp[3] != 0x00) {
        mp_raise_msg_varg(&mp_type_OSError, MP_ERROR_TEXT("CONNACK error: %d"), resp[3]);
    }
    
    return mp_const_none;
}

STATIC mp_obj_t mqtt_disconnect(mp_obj_t self_in) {
    mp_obj_mqtt_client_t *self = MP_OBJ_TO_PTR(self_in);
    
    uint8_t disconnect[] = {0xE0, 0x00};
    mqtt_write_bytes(self, disconnect, sizeof(disconnect));
    
    mp_obj_t close_func = mp_load_attr(self->sock, qstr_from_str("close"));
    mp_call_function_0(close_func);
    
    return mp_const_none;
}

STATIC mp_obj_t mqtt_ping(mp_obj_t self_in) {
    mp_obj_mqtt_client_t *self = MP_OBJ_TO_PTR(self_in);
    
    uint8_t ping[] = {0xC0, 0x00};
    mqtt_write_bytes(self, ping, sizeof(ping));
    
    return mp_const_none;
}

STATIC mp_obj_t mqtt_publish(size_t n_args, const mp_obj_t *args) {
    mp_obj_mqtt_client_t *self = MP_OBJ_TO_PTR(args[0]);
    mp_obj_t topic = args[1];
    mp_obj_t msg = args[2];
    
    bool retain = false;
    uint8_t qos = 0;
    
    if (n_args > 3) {
        retain = mp_obj_is_true(args[3]);
    }
    if (n_args > 4) {
        qos = mp_obj_get_int(args[4]) & 0x03;
    }
    
    size_t topic_len, msg_len;
    const char *topic_str = mp_obj_str_get_data(topic, &topic_len);
    const char *msg_str = mp_obj_str_get_data(msg, &msg_len);
    
    uint8_t cmd = 0x30 | (qos << 1);
    if (retain) {
        cmd |= 0x01;
    }
    
    size_t pkt_len = 2 + topic_len + msg_len;
    if (qos > 0) {
        pkt_len += 2;
    }
    
    mqtt_write_byte(self, cmd);
    
    while (pkt_len > 0x7F) {
        mqtt_write_byte(self, (pkt_len & 0x7F) | 0x80);
        pkt_len >>= 7;
    }
    mqtt_write_byte(self, pkt_len);
    
    mqtt_write_byte(self, topic_len >> 8);
    mqtt_write_byte(self, topic_len & 0xFF);
    mqtt_write_bytes(self, (const uint8_t *)topic_str, topic_len);
    
    if (qos > 0) {
        self->pid = (self->pid + 1) & 0xFFFF;
        mqtt_write_byte(self, self->pid >> 8);
        mqtt_write_byte(self, self->pid & 0xFF);
    }
    
    mqtt_write_bytes(self, (const uint8_t *)msg_str, msg_len);
    
    if (qos == 1) {
        uint8_t resp[4];
        mqtt_read_bytes(self, resp, 4);
        if (resp[0] != 0x40 || resp[1] != 0x02) {
            mp_raise_msg(&mp_type_OSError, MP_ERROR_TEXT("Invalid PUBACK"));
        }
    } else if (qos == 2) {
        mp_raise_NotImplementedError(MP_ERROR_TEXT("QoS 2 not supported"));
    }
    
    return mp_const_none;
}

STATIC mp_obj_t mqtt_subscribe(mp_obj_t self_in, mp_obj_t topic_in, mp_obj_t qos_in) {
    mp_obj_mqtt_client_t *self = MP_OBJ_TO_PTR(self_in);
    
    size_t topic_len;
    const char *topic = mp_obj_str_get_data(topic_in, &topic_len);
    uint8_t qos = mp_obj_get_int(qos_in) & 0x03;
    
    self->pid = (self->pid + 1) & 0xFFFF;
    
    size_t pkt_len = 2 + 2 + topic_len + 1;
    
    mqtt_write_byte(self, 0x82);
    mqtt_write_byte(self, pkt_len);
    mqtt_write_byte(self, self->pid >> 8);
    mqtt_write_byte(self, self->pid & 0xFF);
    mqtt_write_byte(self, topic_len >> 8);
    mqtt_write_byte(self, topic_len & 0xFF);
    mqtt_write_bytes(self, (const uint8_t *)topic, topic_len);
    mqtt_write_byte(self, qos);
    
    uint8_t resp[5];
    mqtt_read_bytes(self, resp, 5);
    
    if (resp[0] != 0x90 || resp[1] != 0x03) {
        mp_raise_msg(&mp_type_OSError, MP_ERROR_TEXT("Invalid SUBACK"));
    }
    
    if (resp[4] == 0x80) {
        mp_raise_msg(&mp_type_OSError, MP_ERROR_TEXT("Subscribe failed"));
    }
    
    return mp_const_none;
}

STATIC mp_obj_t mqtt_set_callback(mp_obj_t self_in, mp_obj_t cb_in) {
    mp_obj_mqtt_client_t *self = MP_OBJ_TO_PTR(self_in);
    self->cb = cb_in;
    return mp_const_none;
}

STATIC mp_obj_t mqtt_set_last_will(size_t n_args, const mp_obj_t *args) {
    mp_obj_mqtt_client_t *self = MP_OBJ_TO_PTR(args[0]);
    self->lw_topic = args[1];
    self->lw_msg = args[2];
    self->lw_qos = (n_args > 3) ? mp_obj_get_int(args[3]) : 0;
    self->lw_retain = (n_args > 4) ? mp_obj_is_true(args[4]) : false;
    return mp_const_none;
}

STATIC mp_obj_t mqtt_check_msg(mp_obj_t self_in) {
    mp_obj_mqtt_client_t *self = MP_OBJ_TO_PTR(self_in);
    
    mp_obj_t setblocking_func = mp_load_attr(self->sock, qstr_from_str("setblocking"));
    mp_call_function_1(setblocking_func, mp_obj_new_bool(false));
    
    mp_obj_t result = mp_const_none;
    
    uint8_t cmd;
    int errcode;
    mp_uint_t ret = mp_stream_rw(self->sock, &cmd, 1, &errcode, MP_STREAM_RW_READ);
    if (ret == 1) {
        if ((cmd & 0xF0) == 0x30) {
            uint8_t sz = mqtt_read_byte(self);
            size_t topic_len = (mqtt_read_byte(self) << 8) | mqtt_read_byte(self);
            
            uint8_t *topic = m_new(uint8_t, topic_len);
            mqtt_read_bytes(self, topic, topic_len);
            
            size_t msg_len = sz - topic_len - 2;
            if ((cmd & 0x06) != 0) {
                mqtt_read_byte(self);  // pid high byte
                mqtt_read_byte(self);  // pid low byte
                msg_len -= 2;
            }
            
            uint8_t *msg = m_new(uint8_t, msg_len);
            mqtt_read_bytes(self, msg, msg_len);
            
            if (self->cb != mp_const_none) {
                mp_obj_t cb_args[2] = {
                    mp_obj_new_bytes(topic, topic_len),
                    mp_obj_new_bytes(msg, msg_len)
                };
                mp_call_function_n_kw(self->cb, 2, 0, cb_args);
            }
            
            m_del(uint8_t, topic, topic_len);
            m_del(uint8_t, msg, msg_len);
            
            result = mp_const_true;
        } else if (cmd == 0xD0) {
            mqtt_read_byte(self);
            uint8_t pong[] = {0xC0, 0x00};
            mqtt_write_bytes(self, pong, sizeof(pong));
        }
    }
    
    mp_call_function_1(setblocking_func, mp_obj_new_bool(true));
    
    return result;
}

STATIC mp_obj_t mqtt_wait_msg(mp_obj_t self_in) {
    mp_obj_mqtt_client_t *self = MP_OBJ_TO_PTR(self_in);
    
    uint8_t cmd = mqtt_read_byte(self);
    
    if ((cmd & 0xF0) == 0x30) {
        uint8_t sz = mqtt_read_byte(self);
        size_t topic_len = (mqtt_read_byte(self) << 8) | mqtt_read_byte(self);
        
        uint8_t *topic = m_new(uint8_t, topic_len);
        mqtt_read_bytes(self, topic, topic_len);
        
        size_t msg_len = sz - topic_len - 2;
        if ((cmd & 0x06) != 0) {
            mqtt_read_byte(self);  // pid high byte
            mqtt_read_byte(self);  // pid low byte
            msg_len -= 2;
        }
        
        uint8_t *msg = m_new(uint8_t, msg_len);
        mqtt_read_bytes(self, msg, msg_len);
        
        if (self->cb != mp_const_none) {
            mp_obj_t cb_args[2] = {
                mp_obj_new_bytes(topic, topic_len),
                mp_obj_new_bytes(msg, msg_len)
            };
            mp_call_function_n_kw(self->cb, 2, 0, cb_args);
        }
        
        m_del(uint8_t, topic, topic_len);
        m_del(uint8_t, msg, msg_len);
    } else if (cmd == 0xD0) {
        mqtt_read_byte(self);
        uint8_t pong[] = {0xC0, 0x00};
        mqtt_write_bytes(self, pong, sizeof(pong));
    }
    
    return mp_const_none;
}

STATIC MP_DEFINE_CONST_FUN_OBJ_VAR_BETWEEN(mqtt_connect_obj, 1, 2, mqtt_connect);
STATIC MP_DEFINE_CONST_FUN_OBJ_1(mqtt_disconnect_obj, mqtt_disconnect);
STATIC MP_DEFINE_CONST_FUN_OBJ_1(mqtt_ping_obj, mqtt_ping);
STATIC MP_DEFINE_CONST_FUN_OBJ_VAR_BETWEEN(mqtt_publish_obj, 3, 5, mqtt_publish);
STATIC MP_DEFINE_CONST_FUN_OBJ_3(mqtt_subscribe_obj, mqtt_subscribe);
STATIC MP_DEFINE_CONST_FUN_OBJ_2(mqtt_set_callback_obj, mqtt_set_callback);
STATIC MP_DEFINE_CONST_FUN_OBJ_VAR_BETWEEN(mqtt_set_last_will_obj, 3, 5, mqtt_set_last_will);
STATIC MP_DEFINE_CONST_FUN_OBJ_1(mqtt_check_msg_obj, mqtt_check_msg);
STATIC MP_DEFINE_CONST_FUN_OBJ_1(mqtt_wait_msg_obj, mqtt_wait_msg);

STATIC const mp_rom_map_elem_t mqtt_locals_dict_table[] = {
    { MP_ROM_QSTR(MP_QSTR_connect), MP_ROM_PTR(&mqtt_connect_obj) },
    { MP_ROM_QSTR(MP_QSTR_disconnect), MP_ROM_PTR(&mqtt_disconnect_obj) },
    { MP_ROM_QSTR(qstr_from_str("ping")), MP_ROM_PTR(&mqtt_ping_obj) },
    { MP_ROM_QSTR(qstr_from_str("publish")), MP_ROM_PTR(&mqtt_publish_obj) },
    { MP_ROM_QSTR(qstr_from_str("subscribe")), MP_ROM_PTR(&mqtt_subscribe_obj) },
    { MP_ROM_QSTR(qstr_from_str("set_callback")), MP_ROM_PTR(&mqtt_set_callback_obj) },
    { MP_ROM_QSTR(qstr_from_str("set_last_will")), MP_ROM_PTR(&mqtt_set_last_will_obj) },
    { MP_ROM_QSTR(qstr_from_str("check_msg")), MP_ROM_PTR(&mqtt_check_msg_obj) },
    { MP_ROM_QSTR(qstr_from_str("wait_msg")), MP_ROM_PTR(&mqtt_wait_msg_obj) },
};
STATIC MP_DEFINE_CONST_DICT(mqtt_locals_dict, mqtt_locals_dict_table);

STATIC const mp_obj_type_t mqtt_client_type = {
    { &mp_type_type },
    .name = qstr_from_str("MQTTClient"),
    .make_new = mqtt_make_new,
    .locals_dict = (mp_obj_dict_t*)&mqtt_locals_dict,
};

STATIC const mp_rom_map_elem_t mp_module_umqtt_globals_table[] = {
    { MP_ROM_QSTR(MP_QSTR___name__), MP_ROM_QSTR(qstr_from_str("umqtt")) },
    { MP_ROM_QSTR(qstr_from_str("MQTTClient")), MP_ROM_PTR(&mqtt_client_type) },
};

STATIC MP_DEFINE_CONST_DICT(mp_module_umqtt_globals, mp_module_umqtt_globals_table);

const mp_obj_module_t mp_module_umqtt = {
    .base = { &mp_type_module },
    .globals = (mp_obj_dict_t*)&mp_module_umqtt_globals,
};

MP_REGISTER_MODULE(qstr_from_str("umqtt"), mp_module_umqtt, MICROPY_PY_UMQTT);

#endif // MICROPY_PY_UMQTT