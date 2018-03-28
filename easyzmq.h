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

#include <string.h>
#include <error.h>

#define clean_errno() (errno == 0 ? "None" : strerror(errno))

#ifdef __cplusplus
extern "C" {
#endif

    typedef struct _ZmqSource GZmqSource;
    
    GZmqSource * g_zmq_source_new(GMainContext *context, void *zmq_context, void *zmq_socket, size_t max_mem_size);
    
    typedef gboolean (*GZmqSourceFunc) (GZmqSource *zmq_source, void *buf, size_t len, gpointer user_data);
    
    gboolean send_zmq_req(GZmqSource *zmq_source, guint timeoutms, void *buf, size_t len, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify);
    
    gboolean recv_zmq_sub(GZmqSource *zmq_source, const gchar* filter, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify);
    gboolean send_zmq_pub(GZmqSource *zmq_source, void *buf, size_t len);
    
    gboolean recv_zmq_resp(GZmqSource *zmq_source, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify);
    gboolean send_zmq_resp(GZmqSource *zmq_source, void *buf, size_t len);
    
    void g_zmq_source_destroy(GZmqSource *self);

    static inline void g_zmq_source_close(GZmqSource **self) { if (self != NULL) { g_zmq_source_destroy(*self); *self = NULL;} }
    
    void g_zmq_source_ref(GZmqSource *self);
    
    void g_zmq_source_unref(GZmqSource *self);

#ifdef __cplusplus
}
#endif

#endif /* EASYZMQ_H */

