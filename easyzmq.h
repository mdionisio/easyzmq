/*
 * File:   easyzmq.h
 * Author: Michele Dionisio
 *
 * Created on 5 dicembre 2016, 10.22
 */

#ifndef EASYZMQ_H
#define EASYZMQ_H

#include <gio/gio.h>
#include <glib.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _ZmqSource GZmqSource;

#define EASY_ZMQ_ERROR easyzmq_error_quark()

GQuark easyzmq_error_quark(void);

typedef enum { EASY_ZMQ_PUB, EASY_ZMQ_SUB, EASY_ZMQ_REQ, EASY_ZMQ_REP, EASY_ZMQ_PAIR, EASY_ZMQ_ROUTER, EASY_ZMQ_UNKNOWN } easyzmq_socket_t;

typedef enum {
    EASYZMQ_EVENT_UNKNOWN = 0,
    EASYZMQ_EVENT_CONNECTED,
    EASYZMQ_EVENT_CONNECT_DELAYED, // DISCARDED from callback
    EASYZMQ_EVENT_CONNECT_RETRIED, // DISCARDED from callback
    EASYZMQ_EVENT_LISTENING,
    EASYZMQ_EVENT_BIND_FAILED,
    EASYZMQ_EVENT_ACCEPTED,
    EASYZMQ_EVENT_ACCEPT_FAILED,
    EASYZMQ_EVENT_CLOSED, // DISCARDED from callback
    EASYZMQ_EVENT_CLOSE_FAILED,
    EASYZMQ_EVENT_DISCONNECTED,
    EASYZMQ_EVENT_MONITOR_STOPPED
} easyzmq_monitor_event_t;

void easy_zmq_init(void);
void easy_zmq_deinit(void);
void *get_zmq_context(void);

GZmqSource *g_zmq_source_req_new(size_t max_mem_size, GMainContext *context) G_GNUC_MALLOC;
GZmqSource *g_zmq_source_rep_new(size_t max_mem_size, GMainContext *context) G_GNUC_MALLOC;
GZmqSource *g_zmq_source_router_new(size_t max_mem_size, GMainContext *context) G_GNUC_MALLOC;
GZmqSource *g_zmq_source_pub_new(GMainContext *context) G_GNUC_MALLOC;
GZmqSource *g_zmq_source_sub_new(size_t max_mem_size, GMainContext *context) G_GNUC_MALLOC;
GZmqSource *g_zmq_source_pair_new(size_t max_mem_size, GMainContext *context) G_GNUC_MALLOC;

typedef void (*GZmqMonitorFunc)(GZmqSource *zmq_source, easyzmq_monitor_event_t event, guint32 value, const guint8 *address, int len);
GZmqSource *get_zmq_socket_monitor(GZmqSource *zmq_source, GZmqMonitorFunc cp) G_GNUC_MALLOC;

gboolean g_easyzmq_bind(GZmqSource *source, const char *addr, GError **error);
gboolean g_easyzmq_unbind(GZmqSource *source, const char *addr, GError **error);
gboolean g_easyzmq_connect(GZmqSource *source, const char *addr, GError **error);
gboolean g_easyzmq_disconnect(GZmqSource *source, const char *addr, GError **error);

gpointer g_easyzmq_get_userdata(GZmqSource *self);
gboolean g_easyzmq_set_userdata(GZmqSource *self, gpointer userdata, GDestroyNotify destroy);

typedef gboolean (*GZmqSourceFunc)(GZmqSource *zmq_source, const guint8 *buf, int len, gboolean more, gpointer user_data);
typedef gboolean (*GZmqSourceIdentityFunc)(GZmqSource *zmq_source, void *identity, const guint8 *buf, int len, gboolean more, gpointer user_data);

/** Send message on zmq req socket
 * zmq_source: self
 * timeoutms: timeout in ms (0 means no timeout)
 * buf: pointer to the byte array to send (the buf is not used any more after the function return)
 * len: size of the buffer to send
 * cb: is the callback to call when there is a result
 * user_data: is send to cb
 * notify: destroy function to destroy user_data when not used any more
 */
gboolean send_zmq_req(GZmqSource *zmq_source, guint timeoutms, const guint8 *buf, int len, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify);
gboolean send_zmq_req_take(GZmqSource *zmq_source, guint timeoutms, guint8 *buf, int len, GDestroyNotify destroy_data, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify);

void send_zmq_req_async(GZmqSource *req, guint timeoutms, const guint8 *buf, int buf_length, GAsyncReadyCallback _callback_, gpointer _user_data_);
void send_zmq_req_take_async(GZmqSource *req, guint timeoutms, guint8 *buf, int buf_length, GAsyncReadyCallback _callback_, gpointer _user_data_);
void send_zmq_req_finish(GZmqSource *req, GAsyncResult *_res_, const guint8 **bufout, int *bufout_length, gboolean *more);

static inline gboolean send_zmq_req_easy(GZmqSource *zmq_source, guint timeoutms, const guint8 *buf, int len, GZmqSourceFunc cb, gpointer user_data) { return send_zmq_req(zmq_source, timeoutms, buf, len, cb, user_data, NULL); }

static inline gboolean send_zmq_req_take_easy(GZmqSource *zmq_source, guint timeoutms, guint8 *buf, int len, GDestroyNotify destroy_data, GZmqSourceFunc cb, gpointer user_data) {
    return send_zmq_req_take(zmq_source, timeoutms, buf, len, destroy_data, cb, user_data, NULL);
}

gboolean subscribe_zmq_sub(GZmqSource *zmq_source, const guint8 *filter, int len, GError **error);
gboolean unsubscribe_zmq_sub(GZmqSource *zmq_source, const guint8 *filter, int len, GError **error);
gboolean recv_zmq_sub(GZmqSource *zmq_source, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify);

gboolean send_zmq_pub(GZmqSource *zmq_source, const guint8 *buf, int len);
gboolean send_zmq_pub_take(GZmqSource *zmq_source, guint8 *buf, int len, GDestroyNotify destroy_data);

gboolean recv_zmq_rep(GZmqSource *zmq_source, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify);
gboolean send_zmq_rep(GZmqSource *zmq_source, const guint8 *buf, int len);
gboolean send_zmq_rep_take(GZmqSource *zmq_source, guint8 *buf, int len, GDestroyNotify destroy_data);

gboolean recv_zmq_router(GZmqSource *zmq_source, GZmqSourceIdentityFunc cb, gpointer user_data, GDestroyNotify notify);
gboolean send_zmq_router(GZmqSource *zmq_source, void *identity, const guint8 *buf, int len);
gboolean send_zmq_router_take(GZmqSource *zmq_source, void *identity, guint8 *buf, int len, GDestroyNotify destroy_data);
void destroy_identity(void *identity);

gboolean recv_zmq_pair(GZmqSource *zmq_source, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify);
gboolean send_zmq_pair(GZmqSource *zmq_source, const guint8 *buf, int len);
gboolean send_zmq_pair_take(GZmqSource *zmq_source, guint8 *buf, int len, GDestroyNotify destroy_data);

void g_zmq_source_destroy(GZmqSource *self);

static inline void g_zmq_source_close(GZmqSource **self) {
    if (G_LIKELY(self != NULL)) {
        g_zmq_source_destroy(*self);
        *self = NULL;
    }
}

GZmqSource *g_zmq_source_ref(GZmqSource *self);

void g_zmq_source_unref(GZmqSource *self);

#ifdef __cplusplus
}
#endif

#endif /* EASYZMQ_H */
