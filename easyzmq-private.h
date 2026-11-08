/*
 * File:   easyzmq-private.h
 * Author: Michele Dionisio
 *
 * internal definitions shared between the easyzmq source files (not installed)
 */

#ifndef EASYZMQ_PRIVATE_H
#define EASYZMQ_PRIVATE_H

#include "easyzmq.h"

#include <glib.h>
#include <zmq.h>

struct _ZmqSource {
    GSource source;
    gpointer fdtag;
    int fd;
    void *zmq_owned_socket;
    void *msg_receive;
    size_t size_receive;
    gint ref_count; // atomic
    easyzmq_socket_t type;
    int send_flags; // ZMQ_DONTWAIT (default) or 0 (blocking send)
    GZmqSource *zmq_source_monitor;
    gpointer user_data;
    GDestroyNotify destroy_user_data;

    // message under dispatch (valid only during the receive callback)
    zmq_msg_t *current_msg;
    const guint8 *current_buf;
    int current_len;

    // receive paused: messages stay queued in zmq
    gboolean paused;
    // state of the async receive (easyzmqtask.c), NULL if not used
    gpointer async_recv;
};

// types that can use the generic receive (callback, bytes, async)
static inline gboolean _easyzmq_type_can_recv(easyzmq_socket_t type) {
    switch (type) {
    case EASY_ZMQ_SUB:
    case EASY_ZMQ_PAIR:
    case EASY_ZMQ_REP:
    case EASY_ZMQ_DEALER:
    case EASY_ZMQ_PULL:
    case EASY_ZMQ_XPUB:
    case EASY_ZMQ_XSUB:
        return TRUE;
    default:
        return FALSE;
    }
}

// types that can use the generic send
static inline gboolean _easyzmq_type_can_send(easyzmq_socket_t type) {
    switch (type) {
    case EASY_ZMQ_PUB:
    case EASY_ZMQ_PAIR:
    case EASY_ZMQ_REP:
    case EASY_ZMQ_DEALER:
    case EASY_ZMQ_PUSH:
    case EASY_ZMQ_XPUB:
    case EASY_ZMQ_XSUB:
        return TRUE;
    default:
        return FALSE;
    }
}

#endif /* EASYZMQ_PRIVATE_H */
