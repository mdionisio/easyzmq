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
    gint ref_count;
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

#endif /* EASYZMQ_PRIVATE_H */
