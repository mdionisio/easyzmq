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

typedef enum { EASY_ZMQ_PUB,
               EASY_ZMQ_SUB,
               EASY_ZMQ_REQ,
               EASY_ZMQ_REP,
               EASY_ZMQ_PAIR,
               EASY_ZMQ_ROUTER,
               EASY_ZMQ_UNKNOWN,
               // added after UNKNOWN to keep the old values
               EASY_ZMQ_DEALER,
               EASY_ZMQ_PUSH,
               EASY_ZMQ_PULL,
               EASY_ZMQ_XPUB,
               EASY_ZMQ_XSUB } easyzmq_socket_t;

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

/** Reference counted initialization: every easy_zmq_init / easy_zmq_init_with_context has
 * to be balanced by easy_zmq_deinit. The context is terminated by the last easy_zmq_deinit
 * (all the sources have to be closed before, zmq_ctx_term blocks until all sockets are closed).
 */
void easy_zmq_init(void);
/** As easy_zmq_init but using a zmq context created by the user (never terminated by easyzmq).
 * If easyzmq is already initialized the existing context is used.
 */
void easy_zmq_init_with_context(void *context);
void easy_zmq_deinit(void);
void *get_zmq_context(void);

GZmqSource *g_zmq_source_req_new(size_t max_mem_size, GMainContext *context) G_GNUC_MALLOC;
GZmqSource *g_zmq_source_rep_new(size_t max_mem_size, GMainContext *context) G_GNUC_MALLOC;
GZmqSource *g_zmq_source_router_new(size_t max_mem_size, GMainContext *context) G_GNUC_MALLOC;
GZmqSource *g_zmq_source_pub_new(GMainContext *context) G_GNUC_MALLOC;
GZmqSource *g_zmq_source_sub_new(size_t max_mem_size, GMainContext *context) G_GNUC_MALLOC;
GZmqSource *g_zmq_source_pair_new(size_t max_mem_size, GMainContext *context) G_GNUC_MALLOC;
GZmqSource *g_zmq_source_dealer_new(size_t max_mem_size, GMainContext *context) G_GNUC_MALLOC;
GZmqSource *g_zmq_source_push_new(GMainContext *context) G_GNUC_MALLOC;
GZmqSource *g_zmq_source_pull_new(size_t max_mem_size, GMainContext *context) G_GNUC_MALLOC;
GZmqSource *g_zmq_source_xpub_new(size_t max_mem_size, GMainContext *context) G_GNUC_MALLOC;
GZmqSource *g_zmq_source_xsub_new(size_t max_mem_size, GMainContext *context) G_GNUC_MALLOC;

/** Create a source from a zmq socket created by the user (e.g. to set options before
 * connect/bind). The socket has to be created on get_zmq_context().
 * The ownership of zmq_socket is taken also in case of error (the socket is closed).
 * Returns NULL if the socket type is not supported.
 */
GZmqSource *g_zmq_source_new_from_socket(void *zmq_socket, size_t max_mem_size, GMainContext *context) G_GNUC_MALLOC;

easyzmq_socket_t g_easyzmq_get_socket_type(GZmqSource *self);

/** By default all the send are not blocking (ZMQ_DONTWAIT): if the message can not be queued
 * (no peer, high water mark reached) the send function returns FALSE instead of blocking
 * the main loop. With blocking == TRUE the old behaviour is restored (on REQ socket the send
 * timeout is the request timeout).
 */
void g_easyzmq_set_blocking_send(GZmqSource *self, gboolean blocking);
gboolean g_easyzmq_get_blocking_send(GZmqSource *self);

/** zmq socket options (option is a ZMQ_* constant of zmq.h). On error the GError code is the zmq errno. */
gboolean g_easyzmq_set_option_int(GZmqSource *self, int option, int value, GError **error);
gboolean g_easyzmq_get_option_int(GZmqSource *self, int option, int *value, GError **error);
gboolean g_easyzmq_set_option_int64(GZmqSource *self, int option, gint64 value, GError **error);
gboolean g_easyzmq_get_option_int64(GZmqSource *self, int option, gint64 *value, GError **error);
gboolean g_easyzmq_set_option_bytes(GZmqSource *self, int option, const guint8 *value, gsize size, GError **error);
gboolean g_easyzmq_set_option_string(GZmqSource *self, int option, const gchar *value, GError **error);
gchar *g_easyzmq_get_option_string(GZmqSource *self, int option, GError **error) G_GNUC_MALLOC;

/** The raw zmq socket (still owned by the source). Use it only for what easyzmq does not
 * offer: send/receive on it bypass the source and break its state.
 */
void *g_easyzmq_get_socket(GZmqSource *self);

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

/* GBytes API: zero copy send (the message keeps a reference of the bytes) and receive
 * (without pre-allocated memory the received data is not copied).
 */
typedef gboolean (*GZmqSourceBytesFunc)(GZmqSource *zmq_source, GBytes *data, gboolean more, gpointer user_data);

/** register the receive callback (all the types of g_easyzmq_recv), data == NULL means receive error */
gboolean g_easyzmq_recv_bytes(GZmqSource *zmq_source, GZmqSourceBytesFunc cb, gpointer user_data, GDestroyNotify notify);
/** send (all the types of g_easyzmq_send) */
gboolean g_easyzmq_send_bytes(GZmqSource *zmq_source, GBytes *bytes);
gboolean send_zmq_req_bytes(GZmqSource *zmq_source, guint timeoutms, GBytes *bytes, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify);
gboolean send_zmq_router_bytes(GZmqSource *zmq_source, void *identity, GBytes *bytes);

/** Return the frame under receive as GBytes (new reference). Valid only inside a receive
 * callback. Without pre-allocated memory (max_mem_size == 0) the data is not copied.
 */
GBytes *g_easyzmq_get_current_bytes(GZmqSource *self);

/** Pause/resume the receive: when paused the messages stay queued in zmq (up to the high
 * water mark) and no callback is called.
 */
void g_easyzmq_set_paused(GZmqSource *self, gboolean paused);
gboolean g_easyzmq_get_paused(GZmqSource *self);

/* GTask based async API.
 *
 * request (REQ socket): send request and wait the answer.
 *   errors: G_IO_ERROR_TIMED_OUT (timeoutms elapsed), G_IO_ERROR_CANCELLED,
 *           EASY_ZMQ_ERROR (send error, code is the zmq errno).
 *   With timeoutms == 0 the socket is in strict REQ mode: after a cancel a new request can
 *   be sent only when the old answer is received (use a timeout to avoid it).
 *
 * receive (all the types of g_easyzmq_recv): wait the next message. More receive can be pending, they are
 *   completed in order. When nobody is waiting the source is paused (messages stay in zmq).
 *   Using recv_zmq_* (callback API) on the same source closes the pending receive
 *   (G_IO_ERROR_CLOSED).
 *
 * _finish returns the message (multipart message: frames concatenated),
 * _multipart_finish returns a GPtrArray of GBytes (one for each frame).
 */
void g_easyzmq_request_async(GZmqSource *req, GBytes *request, guint timeoutms, GCancellable *cancellable, GAsyncReadyCallback callback, gpointer user_data);
GBytes *g_easyzmq_request_finish(GZmqSource *req, GAsyncResult *result, GError **error);
GPtrArray *g_easyzmq_request_multipart_finish(GZmqSource *req, GAsyncResult *result, GError **error);

void g_easyzmq_receive_async(GZmqSource *source, GCancellable *cancellable, GAsyncReadyCallback callback, gpointer user_data);
GBytes *g_easyzmq_receive_finish(GZmqSource *source, GAsyncResult *result, GError **error);
GPtrArray *g_easyzmq_receive_multipart_finish(GZmqSource *source, GAsyncResult *result, GError **error);

/* Generic receive/send, for all the socket types:
 *   receive: SUB, PAIR, REP, DEALER, PULL, XPUB (subscriptions), XSUB
 *   send:    PUB, PAIR, REP, DEALER, PUSH, XPUB, XSUB (subscriptions: "\x01topic" / "\x00topic")
 * REQ and ROUTER have their own functions (callback per request / identity).
 * A ROUTER source expects REQ peers (envelope with empty delimiter), a DEALER talking to it
 * has to send the empty delimiter frame first (see g_easyzmq_send_more).
 */
gboolean g_easyzmq_recv(GZmqSource *zmq_source, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify);
gboolean g_easyzmq_send(GZmqSource *zmq_source, const guint8 *buf, int len);
gboolean g_easyzmq_send_take(GZmqSource *zmq_source, guint8 *buf, int len, GDestroyNotify destroy_data);

/** Multipart send: send a frame with ZMQ_SNDMORE (len can be 0, e.g. the envelope delimiter).
 * The last frame is sent with any normal send function of the source
 * (g_easyzmq_send*, send_zmq_pub*, send_zmq_pair*, send_zmq_rep*).
 * Same socket types of g_easyzmq_send (not REQ and ROUTER).
 * zmq delivers a multipart message atomically (all the frames or nothing) only when the last
 * frame is sent: if a frame after the first one fails, the message can not be cancelled.
 */
gboolean g_easyzmq_send_more(GZmqSource *zmq_source, const guint8 *buf, int len);
gboolean g_easyzmq_send_bytes_more(GZmqSource *zmq_source, GBytes *bytes);

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
