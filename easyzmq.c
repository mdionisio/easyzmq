
#include "easyzmq.h"

#include <glib.h>
#include <zmq.h>

#include <assert.h>
#include <errno.h>
#include <stdint.h>

#define assert_errno(x, message, ...) do { \
                            int _tmp_ = (x); \
                            if (G_UNLIKELY(!_tmp_)) { \
                                fprintf(stderr, message, ##__VA_ARGS__); \
                            } \
                            assert(_tmp_); \
                        } while (0);

struct _ZmqSource {
    GSource source;
    gpointer fd;
    void *zmq_owned_socket;
    void *zmq_context;
    void *msg_receive;
    size_t size_receive;
};

#define ZMQ_SEND_FLAGS ZMQ_DONTWAIT

static gboolean
_g_zmq_source_prepare(GSource *source, gint *timeout) {
    //g_debug("[%s:%d] (%p)", __func__, __LINE__, source);

    GZmqSource *self = (GZmqSource *) source;

    if (self->zmq_owned_socket != NULL) {
        *timeout = -1;
        int zevents = 0;
        size_t zevents_len = sizeof (zevents);
        assert_errno(zmq_getsockopt(self->zmq_owned_socket, ZMQ_EVENTS, &zevents, &zevents_len) == 0, "error %s[%d] on zmq_getsockopt", clean_errno(), errno);

        //g_debug("[%s:%d] (%p) <- %d", __func__, __LINE__, source, (int)ret);

        return (zevents & (ZMQ_POLLIN | ZMQ_POLLERR));
    } else {
        return FALSE;
    }
}

static gboolean
_g_zmq_source_check(GSource *source) {
    GZmqSource *self = (GZmqSource *) source;
    //g_debug("[%s:%d] (%p)", __func__, __LINE__, source);

    if (self->zmq_owned_socket != NULL) {
        int zevents = 0;
        size_t zevents_len = sizeof (zevents);
        assert_errno(zmq_getsockopt(self->zmq_owned_socket, ZMQ_EVENTS, &zevents, &zevents_len) == 0, "error %s[%d] on zmq_getsockopt", clean_errno(), errno);

        //g_debug("[%s:%d] (%p) <- %d", __func__, __LINE__, source, (int)ret);

        return (zevents & (ZMQ_POLLIN | ZMQ_POLLERR));
    } else {
        return FALSE;
    }
}

static gboolean
_g_zmq_source_dispatch(GSource *source, GSourceFunc callback, gpointer user_data) {
    GZmqSource *self = (GZmqSource *) source;
    GZmqSourceFunc zcallback = (GZmqSourceFunc) callback;

    // we don't check in self->fd with g_source_query_unix_fd
    // but using zeromq
    if (self->zmq_owned_socket == NULL) {
        return G_SOURCE_REMOVE;
    }

    int zevents = 0;
    size_t zevents_len = sizeof (zevents);
    assert_errno(zmq_getsockopt(self->zmq_owned_socket, ZMQ_EVENTS, &zevents, &zevents_len) == 0, "error %s[%d] on zmq_getsockopt", clean_errno(), errno);

    //g_debug("[%s:%d] event %d on file %p", __func__, __LINE__, zevents, self->fd);

    if (zevents & ZMQ_POLLIN) {
        //g_debug("[%s:%d] ZMQ_POLLIN", __func__, __LINE__);
        
            if (self->msg_receive != NULL) {
                int rc = zmq_recv(self->zmq_owned_socket, self->msg_receive, self->size_receive, 0 /* WAIT BECAUSE I ALREADY CHECK THAT THERE IS DATA */);
                if ((rc < 0) || (rc > self->size_receive)) {
                    g_info("[%s:%d] error receiving zmq message (%d,%zu): %s (%d)", __func__, __LINE__, rc, self->size_receive, clean_errno(), errno);
                    return zcallback(self, NULL, 0, user_data);
                }
                if (G_LIKELY(zcallback != NULL)) {
                    //g_debug("[%s:%d] received zmq message", __func__, __LINE__);
                    gboolean ret = zcallback(self,
                            self->msg_receive,
                            rc,
                            user_data);
                    return ret;
                } else {
                    g_debug("[%s:%d] receive message when there is no subscription on it", __func__, __LINE__);

                    return G_SOURCE_REMOVE;
                }
            } else {
                zmq_msg_t msg;
                if (zmq_msg_init (&msg) == 0) {
                    ssize_t size = zmq_recvmsg(self->zmq_owned_socket, &msg, 0 /* WAIT BECAUSE I ALREADY CHECK THAT THERE IS DATA */);
                    gboolean ret = FALSE;
                    if (size < 0) {
                        g_info("[%s:%d] error receiving zmq message: %s (%d)", __func__, __LINE__, clean_errno(), errno);
                        ret = zcallback(self, NULL, 0, user_data);
                    } else {
                        //g_debug("[%s:%d] received zmq message", __func__, __LINE__);
                        if (G_LIKELY(zcallback != NULL)) {
                            ret = zcallback(self,
                                            zmq_msg_data(&msg),
                                            zmq_msg_size(&msg),
                                            user_data);
                        } else {
                            g_debug("[%s:%d] receive message when there is no subscription on it", __func__, __LINE__);

                            ret = G_SOURCE_REMOVE;
                        }
                    }
                    assert_errno(zmq_msg_close(&msg) == 0, "error %s[%d] on zmq_msg_close", clean_errno(), errno);
                    return ret;

                } else {
                    g_debug("[%s:%d] no memory to receive any message", __func__, __LINE__);
                    return G_SOURCE_REMOVE;
                }
            }
    } else if (zevents & ZMQ_POLLOUT) {
        g_debug("[%s:%d] ZMQ FD no other event maneged yet ZMQ_POLLOUT", __func__, __LINE__);
    } else if (zevents & ZMQ_POLLERR) {
        g_debug("[%s:%d] error", __func__, __LINE__);
        if (G_LIKELY(zcallback != NULL)) {
            return zcallback(self, NULL, 0, user_data);
        } else {
            g_debug("[%s:%d] receive error when there is no subscription on it", __func__, __LINE__);
            return G_SOURCE_REMOVE;
        }
    } else {
        g_debug("[%s:%d] no event", __func__, __LINE__);
    }

    return G_SOURCE_CONTINUE;
}

static void
_g_zmq_source_finalize(GSource *source) {
    GZmqSource *self = (GZmqSource *) source;

    g_debug("[%s:%d] (%p) GZmqSource finalize", __func__, __LINE__, source);
    if (self->fd != NULL) {
        g_source_remove_unix_fd((GSource*) self, self->fd);
        self->fd = NULL;
    }

    if (NULL != self->msg_receive) {
        g_free(self->msg_receive);
        self->msg_receive = NULL;
    }

    if (self->zmq_owned_socket != NULL) {
        g_debug("[%s:%d] (%p) close zmq", __func__, __LINE__, source);
        zmq_close(self->zmq_owned_socket);
        self->zmq_owned_socket = NULL;
    }

}

static GSourceFuncs _g_zmq_source_funcs = {
    _g_zmq_source_prepare,
    _g_zmq_source_check,
    _g_zmq_source_dispatch,
    _g_zmq_source_finalize
};

static gboolean fake_callback(GZmqSource *zmq_source, void *buf, size_t len, gpointer user_data) {
    g_debug("[%s:%d]", __func__, __LINE__);
    return G_SOURCE_CONTINUE;
}

void g_zmq_source_ref(GZmqSource *self) {
    g_return_if_fail(self != NULL);

    g_source_ref((GSource *) self);
}

void g_zmq_source_unref(GZmqSource *self) {
    g_return_if_fail(self != NULL);

    g_source_unref((GSource *) self);
}

void g_zmq_source_destroy(GZmqSource *self) {
    g_return_if_fail(self != NULL);

    g_source_destroy((GSource *) self);
}

void g_zmq_source_close(GZmqSource **self) {
    g_return_if_fail(self != NULL);
    g_zmq_source_destroy(*self);
    g_zmq_source_unref(*self);
    *self = NULL;
}

GZmqSource * g_zmq_source_new(GMainContext *l_context, void *l_zmq_context, void *l_zmq_socket, size_t max_mem_size) {
    GZmqSource *source = NULL;

    g_debug("[%s:%d] GZmqSource creating ...", __func__, __LINE__);

    source = (GZmqSource *) g_source_new(&_g_zmq_source_funcs, sizeof (GZmqSource));

    source->zmq_owned_socket = l_zmq_socket;
    source->zmq_context = l_zmq_context;

    if (max_mem_size > 0) {
        {
            int64_t max_msg_size = (int64_t)max_mem_size;
            if (zmq_setsockopt(l_zmq_socket, ZMQ_MAXMSGSIZE, &max_msg_size, sizeof(max_msg_size)) != 0) {
                g_warning("[%s:%d] GZmqSource error setting max message size %s", __func__, __LINE__, clean_errno());
            }
        }
        source->msg_receive = g_malloc0(max_mem_size);
        source->size_receive = max_mem_size;
    } else {
        source->msg_receive = NULL;
        source->size_receive = 0;
    }
    source->fd = NULL;

    int fd = 0;
    size_t fd_len = sizeof (fd);
    if (zmq_getsockopt(source->zmq_owned_socket, ZMQ_FD, &fd, &fd_len) == 0) {

        g_debug("[%s:%d] GZmqSource zmq fd: %d", __func__, __LINE__, fd);

        if (G_UNLIKELY(fd != 0)) {
            source->fd = g_source_add_unix_fd((GSource *) source, fd, G_IO_IN | G_IO_PRI | G_IO_ERR | G_IO_HUP | G_IO_NVAL);

            g_debug("[%s:%d] g_source_add_unix_fd: %p", __func__, __LINE__, source->fd);
            g_source_set_callback((GSource *) source, (GSourceFunc) fake_callback, NULL, NULL);
            g_debug("[%s:%d] g_source_set_callback", __func__, __LINE__);
            g_source_attach((GSource *) source, l_context);

            g_debug("[%s:%d] g_source_attach", __func__, __LINE__);

        } else {
            g_debug("[%s:%d] GZmqSource create error", __func__, __LINE__);
            g_zmq_source_unref(source);
            source = NULL;
        }
    } else {
        g_debug("[%s:%d] GZmqSource create error: %s", __func__, __LINE__, clean_errno());
        g_zmq_source_unref(source);
        source = NULL;
    }

    g_debug("[%s:%d] GZmqSource created: %p", __func__, __LINE__, source);
    return source;
}

// RESPONSE SOCKET

gboolean recv_zmq_resp(GZmqSource *zmq_source, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify) {
    assert(zmq_source != NULL);
    g_debug("[%s:%d] register for responce receiving [%p]", __func__, __LINE__, zmq_source);
    g_source_set_callback((GSource *) zmq_source, (GSourceFunc) cb, user_data, notify);

    return TRUE;
}

gboolean send_zmq_resp(GZmqSource *zmq_source, void *buf, size_t len) {
    assert(zmq_source != NULL);
    assert(zmq_source->zmq_owned_socket != NULL);
    assert(buf != NULL);
    assert(len > 0);

    zmq_msg_t message;
    zmq_msg_init_size(&message, len);
    memcpy(zmq_msg_data(&message), buf, len);
    int r = zmq_msg_send(&message, zmq_source->zmq_owned_socket, ZMQ_SEND_FLAGS);

    if (G_UNLIKELY(r == -1)) {
        zmq_msg_close(&message);
        g_debug("[%s:%d] send resp message error (%d,%s) [%p,%p]", __func__, __LINE__, errno, strerror(errno), zmq_source, zmq_source->zmq_owned_socket);
        return FALSE;
    } else {
        g_debug("[%s:%d] send resp message ok [%p,%p]", __func__, __LINE__, zmq_source, zmq_source->zmq_owned_socket);
        return TRUE;
    }
}

// REQUEST SOCKET

typedef struct req_callback_data {
    gpointer user_data;
    GDestroyNotify notify;
    GZmqSourceFunc callback;
    GZmqSource *zmq_source;
    guint timeout_handler;
} req_callback_data_t;

void req_callback_data_free(gpointer data) {
    if (data != NULL) {
        req_callback_data_t *pdata = data;
        if (G_LIKELY((pdata->notify != NULL) && (pdata->user_data != NULL))) {
            g_debug("[%s:%d] free user data", __func__, __LINE__);
            pdata->notify(pdata->user_data);
            pdata->user_data = NULL;
            pdata->notify = NULL;
        }
        g_debug("[%s:%d] free all (%p)", __func__, __LINE__, data);
        g_free(data);
    }
}

static gboolean req_callback(GZmqSource *zmq_source, void *buf, size_t len, gpointer user_data) {
    assert(user_data != NULL);
    req_callback_data_t *pdata = user_data;
    g_debug("[%s:%d] (%p)", __func__, __LINE__, zmq_source);
    if (pdata->timeout_handler == 0) {
        g_debug("[%s:%d] timeout already manage so skeep this (%p)", __func__, __LINE__, zmq_source);
    } else {
        g_source_remove(pdata->timeout_handler);
        pdata->timeout_handler = 0;
        if (G_LIKELY(pdata->callback != NULL)) {
            pdata->callback(zmq_source, buf, len, pdata->user_data);
            g_source_set_callback((GSource *) zmq_source, (GSourceFunc) fake_callback, NULL, NULL);
        }
    }
    return G_SOURCE_CONTINUE;
}

gboolean req_timeout_callback(gpointer user_data) {
    assert(user_data != NULL);
    req_callback_data_t *pdata = (req_callback_data_t*) user_data;
    pdata->timeout_handler = 0;
    void * user_data2 = pdata->user_data;
    GDestroyNotify notify2 = pdata->notify;
    GZmqSource *zmq_source = pdata->zmq_source;
    GZmqSourceFunc callback = pdata->callback;
    pdata->notify = NULL;
    pdata->user_data = NULL;
    g_debug("[%s:%d] (%p)", __func__, __LINE__, zmq_source);
    if (G_LIKELY(callback != NULL)) {
        g_debug("[%s:%d] change callback", __func__, __LINE__);
        g_source_set_callback((GSource *) zmq_source, (GSourceFunc) fake_callback, NULL, NULL); // this call means destroy user_data
        // so all the used parameter is saved before
        // use it
        g_debug("[%s:%d] call callback", __func__, __LINE__);
        callback(zmq_source, NULL, 0, user_data2);
    }
    if (G_LIKELY((notify2 != NULL) && (user_data2 != NULL))) {
        g_debug("[%s:%d] free user data", __func__, __LINE__);
        notify2(user_data2);
    }

    return G_SOURCE_REMOVE;
}

gboolean send_zmq_req(GZmqSource *zmq_source, guint timeoutms, void *buf, size_t len, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify) {
    assert(zmq_source != NULL);
    assert(zmq_source->zmq_owned_socket != NULL);
    assert(buf != NULL);
    assert(len > 0);

    gboolean ret = FALSE;

    req_callback_data_t *pdata = g_new0(req_callback_data_t, 1);
    pdata->zmq_source = zmq_source;
    pdata->user_data = user_data;
    pdata->notify = notify;
    pdata->callback = cb;
    pdata->timeout_handler = 0U;

    g_source_set_callback((GSource *) zmq_source, (GSourceFunc) req_callback, pdata, req_callback_data_free);

    zmq_msg_t message;
    zmq_msg_init_size(&message, len);
    memcpy(zmq_msg_data(&message), buf, len);
    int r = zmq_msg_send(&message, zmq_source->zmq_owned_socket, ZMQ_SEND_FLAGS);

    if (G_UNLIKELY(r == -1)) {
        zmq_msg_close(&message);
        g_debug("[%s:%d] send req message error (%d, %s) [%p,%p]", __func__, __LINE__, errno, strerror(errno), zmq_source, zmq_source->zmq_owned_socket);
        g_source_set_callback((GSource *) zmq_source, (GSourceFunc) fake_callback, NULL, NULL);
        g_debug("[%s:%d] error managed", __func__, __LINE__);
    } else {
        g_debug("[%s:%d] send req message ok [%p,%p]", __func__, __LINE__, zmq_source, zmq_source->zmq_owned_socket);
        // add timeout before call error callback
        if (timeoutms != 0) {
            pdata->timeout_handler = g_timeout_add(timeoutms, req_timeout_callback, pdata);
        }
        ret = TRUE;
    }

    return ret;
}

// SUB SOCKET

gboolean recv_zmq_sub(GZmqSource *zmq_source, const gchar* filter, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify) {
    assert(zmq_source != NULL);
    assert(zmq_source->zmq_owned_socket != NULL);

    g_debug("[%s:%d] register for subscriber receiving", __func__, __LINE__);

    if (filter == NULL) {
        assert_errno(zmq_setsockopt(zmq_source->zmq_owned_socket, ZMQ_SUBSCRIBE, "", 0) == 0, "error %s[%d] on zmq_setsockopt", clean_errno(), errno);
    } else {
        assert_errno(zmq_setsockopt(zmq_source->zmq_owned_socket, ZMQ_SUBSCRIBE, filter, strlen(filter)) == 0, "error %s[%d] on zmq_setsockopt", clean_errno(), errno);
    }

    g_source_set_callback((GSource *) zmq_source, (GSourceFunc) cb, user_data, notify);

    return TRUE;
}

// PUB SOCKET

gboolean send_zmq_pub(GZmqSource *zmq_source, void *buf, size_t len) {
    assert(zmq_source != NULL);
    assert(zmq_source->zmq_owned_socket != NULL);
    assert(buf != NULL);
    assert(len > 0);

    gboolean ret = FALSE;

    zmq_msg_t message;
    zmq_msg_init_size(&message, len);
    memcpy(zmq_msg_data(&message), buf, len);
    int r = zmq_msg_send(&message, zmq_source->zmq_owned_socket, ZMQ_SEND_FLAGS);

    g_source_set_callback((GSource *) zmq_source, (GSourceFunc) fake_callback, NULL, NULL);

    if (r == -1) {
        zmq_msg_close(&message);
        g_debug("[%s:%d] send pub message error (%d, %s)", __func__, __LINE__, errno, strerror(errno));
    } else {
        g_debug("[%s:%d] send pub message ok", __func__, __LINE__);
        ret = TRUE;
    }

    return ret;
}