/* 
 * File:   easyzmq.h
 * Author: Michele Dionisio
 *
 * Created on 5 dicembre 2016, 10.22
 */

#ifndef EASYZMQ_H
#define EASYZMQ_H

#include <glib.h>
#include <zmq.h>

#ifdef __cplusplus
extern "C" {
#endif

    typedef struct _ZmqSource GZmqSource;
    
    GZmqSource * g_zmq_source_new(void *zmq_context, void *zmq_socket, size_t max_mem_size, GMainContext *context);
    
    typedef gboolean (*GZmqSourceFunc) (GZmqSource *zmq_source, const void *buf, size_t len, gpointer user_data);

    /** Send message on zmq req socket
     * zmq_source: self
     * timeoutms: timeout in ms (0 means no timeout)
     * buf: pointer to the byte array to send (the buf is not used any more after the function return)
     * len: size of the buffer to send
     * cb: is the callback to call when there is a result
     * user_data: is send to cb
     * notify: destroy function to destroy user_data when not used any more
     */
    gboolean send_zmq_req(GZmqSource *zmq_source, guint timeoutms, const void *buf, size_t len, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify);

    static inline gboolean send_zmq_req_easy(GZmqSource *zmq_source, guint timeoutms, const void *buf, size_t len, GZmqSourceFunc cb, gpointer user_data) {
        return send_zmq_req(zmq_source, timeoutms, buf, len, cb, user_data, NULL);
    }

    gboolean send_zmq_req_take(GZmqSource *zmq_source, guint timeoutms, void *buf, size_t len, GDestroyNotify destroy_data, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify);

    static inline gboolean send_zmq_req_easy_take(GZmqSource *zmq_source, guint timeoutms, void *buf, size_t len, GDestroyNotify destroy_data, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify) {
        return send_zmq_req_take(zmq_source, timeoutms, buf, len, destroy_data, cb, user_data, notify);
    }
    
    gboolean recv_zmq_sub(GZmqSource *zmq_source, const gchar* filter, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify);
    gboolean send_zmq_pub(GZmqSource *zmq_source, const void *buf, size_t len);
    gboolean send_zmq_pub_take(GZmqSource *zmq_source, void *buf, size_t len, GDestroyNotify destroy_data);
    
    gboolean recv_zmq_resp(GZmqSource *zmq_source, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify);
    gboolean send_zmq_resp(GZmqSource *zmq_source, const void *buf, size_t len);
    gboolean send_zmq_resp_take(GZmqSource *zmq_source, void *buf, size_t len, GDestroyNotify destroy_data);
    
    void g_zmq_source_destroy(GZmqSource *self);

    static inline void g_zmq_source_close(GZmqSource **self) { if (self != NULL) { g_zmq_source_destroy(*self); *self = NULL;} }
    
    void g_zmq_source_ref(GZmqSource *self);
    
    void g_zmq_source_unref(GZmqSource *self);

#ifdef __cplusplus
}
#endif

#endif /* EASYZMQ_H */

