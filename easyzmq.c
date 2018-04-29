
#include "easyzmq.h"

#include <glib.h>
#include <zmq.h>
#include <stdint.h>
#include <string.h>

#define clean_errno() (zmq_errno() == 0 ? "None" : zmq_strerror(zmq_errno()))

#define assert_errno(x, message, ...) do { \
                            int _tmp_ = (x); \
                            if (G_UNLIKELY(!_tmp_)) { \
                                fprintf(stderr, message, ##__VA_ARGS__); \
                            } \
                            g_assert(_tmp_); \
                        } while (0);

static int _zmq_setsockopt (void *socket, int option_name, const void *option_value, size_t option_len) {
    int retval = 0;
    do {
        retval = zmq_setsockopt (socket, option_name, option_value, option_len);
    } while ((retval != 0) && (zmq_errno() == EINTR));
    return retval;
}

static guint64 get_unique(void) {
    static guint64 counter = 0;
    return counter++;
}

static void *zmq_context = NULL;

void easy_zmq_init(void) {
    if (zmq_context == NULL) {
        g_debug("create zmq context");
        zmq_context = zmq_ctx_new ();
    }
}

void easy_zmq_deinit(void) {
    if (zmq_context != NULL) {
        g_debug("destroy zmq context");
        zmq_ctx_term (zmq_context);
        zmq_context = NULL;
    }
}

void *get_zmq_context(void) {
    return zmq_context;
}

/**
 * easyzmq_error_quark:
 *
 * Gets the EasyZMQ Error Quark.
 *
 * Returns: a #GQuark.
 **/
G_DEFINE_QUARK (easyzmq-error-quark, easyzmq_error)

struct _ZmqSource {
    GSource source;
    gpointer fdtag;
    int fd;
    void *zmq_owned_socket;
    void *msg_receive;
    size_t size_receive;
    gint ref_count;
    easyzmq_socket_t type;
    GZmqSource *zmq_source_monitor;
    gpointer *user_data;
    GDestroyNotify destroy_user_data;
};

#define ZMQ_SEND_FLAGS ZMQ_DONTWAIT
//#define ZMQ_SEND_FLAGS 0

static void _pollfdOn(GZmqSource *source);
static void _pollfdOff(GZmqSource *source);

static gboolean _g_zmq_source_prepare(GSource *source, gint *timeout) {
    //g_debug("[%s:%d] (%p)", __func__, __LINE__, (void*)source);

    GZmqSource *self = (GZmqSource *) source;

    if (self->zmq_owned_socket != NULL) {
        *timeout = -1;
        int zevents = 0;
        size_t zevents_len = sizeof (zevents);
        assert_errno(zmq_getsockopt(self->zmq_owned_socket, ZMQ_EVENTS, &zevents, &zevents_len) == 0, "error %s[%d] on zmq_getsockopt", clean_errno(), zmq_errno());

        //g_debug("[%s:%d] (%p) <- %d", __func__, __LINE__, (void*)source, (int)ret);

        return (zevents & (ZMQ_POLLIN | ZMQ_POLLERR));
    } else {
        return FALSE;
    }
}

static gboolean _g_zmq_source_check(GSource *source) {
    GZmqSource *self = (GZmqSource *) source;
    //g_debug("[%s:%d] (%p)", __func__, __LINE__, (void*)source);

    if (self->zmq_owned_socket != NULL) {
        int zevents = 0;
        size_t zevents_len = sizeof (zevents);
        assert_errno(zmq_getsockopt(self->zmq_owned_socket, ZMQ_EVENTS, &zevents, &zevents_len) == 0, "error %s[%d] on zmq_getsockopt", clean_errno(), zmq_errno());

        //g_debug("[%s:%d] (%p) <- %d", __func__, __LINE__, (void*)source, (int)ret);

        return (zevents & (ZMQ_POLLIN | ZMQ_POLLERR));
    } else {
        return FALSE;
    }
}

static gboolean _g_zmq_source_dispatch(GSource *source, GSourceFunc callback, gpointer user_data) {
    GZmqSource *self = (GZmqSource *) source;
    GZmqSourceFunc zcallback = (GZmqSourceFunc) callback;

    // we don't check in self->fd with g_source_query_unix_fd
    // but using zeromq
    if (self->zmq_owned_socket == NULL) {
        return G_SOURCE_REMOVE;
    }

    for(;;) {
        int zevents = 0;
        size_t zevents_len = sizeof (zevents);
        assert_errno(zmq_getsockopt(self->zmq_owned_socket, ZMQ_EVENTS, &zevents, &zevents_len) == 0, "error %s[%d] on zmq_getsockopt", clean_errno(), zmq_errno());

        //g_debug("[%s:%d] event %d on file %p", __func__, __LINE__, zevents, (void*)self->fd);

        if (zevents & ZMQ_POLLIN) {
            //g_debug("[%s:%d] ZMQ_POLLIN", __func__, __LINE__);
            
                if (self->msg_receive != NULL) {
                    int rc = zmq_recv(self->zmq_owned_socket, self->msg_receive, self->size_receive, 0 /* WAIT BECAUSE I ALREADY CHECK THAT THERE IS DATA */);
                    if ((rc < 0) || (((size_t)rc) > self->size_receive)) {
                        g_info("[%s:%d] error receiving zmq message (%d,%zu): %s (%d)", __func__, __LINE__, rc, self->size_receive, clean_errno(), zmq_errno());
                        return zcallback(self, NULL, 0, FALSE, user_data);
                    }

                    if (G_LIKELY(zcallback != NULL)) {
                        int more = 0;
                        size_t more_len = sizeof(more);
                        assert_errno(zmq_getsockopt (self->zmq_owned_socket, ZMQ_RCVMORE, &more, &more_len) == 0, "error %s[%d] on zmq_getsockopt", clean_errno(), zmq_errno());

                        //g_debug("[%s:%d] received zmq message", __func__, __LINE__);
                        gboolean ret = zcallback(self,
                                self->msg_receive,
                                rc,
                                (more != 0) ? TRUE : FALSE,
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
                            g_info("[%s:%d] error receiving zmq message: %s (%d)", __func__, __LINE__, clean_errno(), zmq_errno());
                            ret = zcallback(self, NULL, 0, FALSE, user_data);
                        } else {
                            //g_debug("[%s:%d] received zmq message", __func__, __LINE__);
                            if (G_LIKELY(zcallback != NULL)) {
                                ret = zcallback(self,
                                                zmq_msg_data(&msg),
                                                zmq_msg_size(&msg),
                                                zmq_msg_more(&msg),
                                                user_data);
                            } else {
                                g_debug("[%s:%d] receive message when there is no subscription on it", __func__, __LINE__);

                                ret = G_SOURCE_REMOVE;
                            }
                        }
                        //g_debug("destroying received data: %p", self);
                        assert_errno(zmq_msg_close(&msg) == 0, "error %s[%d] on zmq_msg_close", clean_errno(), zmq_errno());
                        return ret;

                    } else {
                        g_debug("[%s:%d] no memory to receive any message", __func__, __LINE__);
                        return G_SOURCE_REMOVE;
                    }
                }
        } else if (zevents & ZMQ_POLLOUT) {
            g_debug("[%s:%d] ZMQ FD no other event maneged yet ZMQ_POLLOUT", __func__, __LINE__);
            break;
        } else if (zevents & ZMQ_POLLERR) {
            g_debug("[%s:%d] error", __func__, __LINE__);
            if (G_LIKELY(zcallback != NULL)) {
                return zcallback(self, NULL, 0, FALSE, user_data);
            } else {
                g_debug("[%s:%d] receive error when there is no subscription on it", __func__, __LINE__);
                return G_SOURCE_REMOVE;
            }
        } else {
            g_debug("[%s:%d] no event", __func__, __LINE__);
            break;
        }
    }

    return G_SOURCE_CONTINUE;
}

static void _g_easyzmq_destroy_userdata(GZmqSource *self) {
    if (self->user_data != NULL) { 
        if (self->destroy_user_data != NULL) {
            self->destroy_user_data(self->user_data);
        } 
        self->user_data = NULL;
    }
}

static void _g_zmq_source_finalize(GSource *source) {
    GZmqSource * const self = (GZmqSource *) source;

    g_debug("[%s:%d] (%p) GZmqSource finalize", __func__, __LINE__, (void*)source);
    _pollfdOff(self);
    self->fd = -1;

    if (NULL != self->msg_receive) {
        g_free(self->msg_receive);
        self->msg_receive = NULL;
    }

    if (self->zmq_source_monitor != NULL) {
        g_zmq_source_unref(self->zmq_source_monitor);
        self->zmq_source_monitor = NULL;
    }
    _g_easyzmq_destroy_userdata(self);

    if (self->zmq_owned_socket != NULL) {
        g_debug("[%s:%d] (%p) close zmq", __func__, __LINE__, (void*)source);
        int linger = 0;
        assert_errno(_zmq_setsockopt(self->zmq_owned_socket, ZMQ_LINGER, &linger, sizeof(linger)) == 0, "error %s[%d] on zmq_setsockopt", clean_errno(), zmq_errno());
        assert_errno(zmq_close(self->zmq_owned_socket) == 0, "error %s[%d] on zmq_close", clean_errno(), zmq_errno());
        self->zmq_owned_socket = NULL;
    }
}

gpointer g_easyzmq_get_userdata(GZmqSource *self) {
    g_assert(self != NULL);
    return self->user_data;
}

gboolean g_easyzmq_set_userdata(GZmqSource *self, gpointer userdata, GDestroyNotify destroy) {
    g_assert(self != NULL);
    _g_easyzmq_destroy_userdata(self);

    self->user_data = userdata;
    self->destroy_user_data = destroy;

    return TRUE;
}


static GSourceFuncs _g_zmq_source_funcs = {
    .prepare = _g_zmq_source_prepare,
    .check = _g_zmq_source_check,
    .dispatch = _g_zmq_source_dispatch,
    .finalize = _g_zmq_source_finalize
};

static gboolean fake_callback(G_GNUC_UNUSED GZmqSource *zmq_source, G_GNUC_UNUSED const guint8 *buf, G_GNUC_UNUSED int len, G_GNUC_UNUSED gboolean more, G_GNUC_UNUSED gpointer user_data) {
    g_warning("[%s:%d] probably data received out of order", __func__, __LINE__);
    //g_assert_not_reached();
    return G_SOURCE_CONTINUE;
}

GZmqSource * g_zmq_source_ref(GZmqSource *self) {
    g_return_val_if_fail(self != NULL, NULL);
    g_warn_if_fail(self->ref_count != 0);

    self->ref_count ++;

    g_source_ref((GSource *) self);
    return self;
}

void g_zmq_source_unref(GZmqSource *self) {
    g_return_if_fail(self != NULL);
    if (self->ref_count > 0) {
        self->ref_count --;
    } else {
        g_warn_if_reached();
    }
    if (self->ref_count == 0) {
        g_source_destroy((GSource *) self);
    } else {
        g_source_unref((GSource *) self);
    }
}

void g_zmq_source_destroy(GZmqSource *self) {
    g_return_if_fail(self != NULL);

    g_source_destroy((GSource *) self);
}

gboolean g_easyzmq_bind (GZmqSource *source, const char *addr, GError **error) {
    g_assert(source != NULL);
    g_assert(addr != NULL);
    g_assert(source->zmq_owned_socket != NULL);
    int rc = zmq_bind (source->zmq_owned_socket, addr);
    if (rc != 0) {
        int zerrno = zmq_errno();
        g_info("[%s:%d] bind error: %d %s", __func__, __LINE__, zerrno, zerrno == 0 ? "None" : zmq_strerror(zerrno));
        g_set_error (error, EASY_ZMQ_ERROR, zerrno,
		     "easyzmq errno: %d %s during bind",
		     zerrno, zmq_strerror(zerrno));
        return FALSE;
    }
    return TRUE;
}

gboolean g_easyzmq_unbind (GZmqSource *source, const char *addr, GError **error) {
    g_assert(source != NULL);
    g_assert(addr != NULL);
    g_assert(source->zmq_owned_socket != NULL);
    int rc = zmq_unbind (source->zmq_owned_socket, addr);
    if (rc != 0) {
        int zerrno = zmq_errno();
        g_info("[%s:%d] unbind error: %d %s", __func__, __LINE__, zerrno, zerrno == 0 ? "None" : zmq_strerror(zerrno));
        g_set_error (error, EASY_ZMQ_ERROR, zerrno,
		     "easyzmq errno: %d %s during unbind",
		     zerrno, zmq_strerror(zerrno));
        return FALSE;
    }
    return TRUE;
}


gboolean g_easyzmq_connect (GZmqSource *source, const char *addr, GError **error) {
    g_assert(source != NULL);
    g_assert(addr != NULL);
    g_assert(source->zmq_owned_socket != NULL);
    int rc = zmq_connect (source->zmq_owned_socket, addr);
    if (rc != 0) {
        int zerrno = zmq_errno();
        g_info("[%s:%d] connect error: %d %s", __func__, __LINE__, zerrno, zerrno == 0 ? "None" : zmq_strerror(zerrno));
        g_set_error (error, EASY_ZMQ_ERROR, zerrno,
		     "easyzmq errno: %d %s during connect",
		     zerrno, zmq_strerror(zerrno));
        return FALSE;
    }
    return TRUE;
}

gboolean g_easyzmq_disconnect (GZmqSource *source, const char *addr, GError **error) {
    g_assert(source != NULL);
    g_assert(addr != NULL);
    g_assert(source->zmq_owned_socket != NULL);
    int rc = zmq_disconnect (source->zmq_owned_socket, addr);
    if (rc != 0) {
        int zerrno = zmq_errno();
        g_info("[%s:%d] disconnect error: %d %s", __func__, __LINE__, zerrno, zerrno == 0 ? "None" : zmq_strerror(zerrno));
        g_set_error (error, EASY_ZMQ_ERROR, zerrno,
		     "easyzmq errno: %d %s during disconnect",
		     zerrno, zmq_strerror(zerrno));
        return FALSE;
    }
    return TRUE;
}

static void _pollfdOn(GZmqSource *source) {
    g_assert(source != NULL);
    g_assert(source->fd > 0);
    g_assert(source->fdtag == NULL);

    source->fdtag = g_source_add_unix_fd((GSource *) source, source->fd, G_IO_IN | G_IO_PRI | G_IO_ERR | G_IO_HUP | G_IO_NVAL);

    g_debug("[%s:%d] %p %p", __func__, __LINE__, (void*)source, (void*)source->fdtag);

    g_assert(source->fdtag != NULL);
}

static void _pollfdOff(GZmqSource *source) {
    g_assert(source != NULL);

    if (source->fdtag != NULL) {
        g_debug("[%s:%d] %p %p", __func__, __LINE__, (void*)source, (void*)source->fdtag);
        g_source_remove_unix_fd((GSource*) source, source->fdtag);
        source->fdtag = NULL;
    }
}

static GZmqSource * g_zmq_source_new(void *l_zmq_socket, size_t max_mem_size, GMainContext *l_context) {
    g_assert(l_zmq_socket != NULL);
    g_assert(zmq_context != NULL);
    if (l_context == NULL) {
        l_context = g_main_context_default();
    }
    g_assert(l_context != NULL);

    GZmqSource *source = NULL;

    g_debug("[%s:%d] GZmqSource creating ...", __func__, __LINE__);

    source = (GZmqSource *) g_source_new(&_g_zmq_source_funcs, sizeof (GZmqSource));

    source->zmq_owned_socket = l_zmq_socket;
    source->type = EASY_ZMQ_UNKNOWN;

    if (max_mem_size > 0) {
        {
            int64_t max_msg_size = (int64_t)max_mem_size;
            if (_zmq_setsockopt(l_zmq_socket, ZMQ_MAXMSGSIZE, &max_msg_size, sizeof(max_msg_size)) != 0) {
                g_warning("[%s:%d] GZmqSource error setting max message size %s", __func__, __LINE__, clean_errno());
            }
        }
        source->msg_receive = g_malloc0(max_mem_size);
        source->size_receive = max_mem_size;
    } else {
        source->msg_receive = NULL;
        source->size_receive = 0;
    }
    source->fdtag = NULL;
    source->zmq_source_monitor = NULL;
    source->fd = -1;
    source->ref_count = 1;

    int fd = -1;
    size_t fd_len = sizeof (fd);
    if (zmq_getsockopt(source->zmq_owned_socket, ZMQ_FD, &fd, &fd_len) == 0) {

        g_debug("[%s:%d] GZmqSource zmq fd: %d", __func__, __LINE__, fd);
        source->fd = fd;

        if (G_UNLIKELY(source->fd > 0)) {            
            g_source_set_callback((GSource *) source, (GSourceFunc) fake_callback, NULL, NULL);
            g_debug("[%s:%d] g_source_set_callback", __func__, __LINE__);
            g_source_attach((GSource *) source, l_context);
            g_source_unref((GSource *) source);

            g_debug("[%s:%d] GZmqSource created and attached: %p ref_count %d", __func__, __LINE__, (void*)source, source->ref_count);

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
    
    return source;
}


GZmqSource * g_zmq_source_req_new(size_t max_mem_size, GMainContext *context) {
    void *zmqsocket = zmq_socket (get_zmq_context(), ZMQ_REQ);
    if (zmqsocket == NULL) {
        int zerrno = zmq_errno();
        g_info("[%s:%d] new req socket error: %d %s", __func__, __LINE__, zerrno, zerrno == 0 ? "None" : zmq_strerror(zerrno));
        return NULL;
    }
    GZmqSource * source = g_zmq_source_new(zmqsocket, max_mem_size, context);
    if (source != NULL) {
        source->type = EASY_ZMQ_REQ;
    }
    return source; 
}

GZmqSource * g_zmq_source_rep_new(size_t max_mem_size, GMainContext *context) {
    void *zmqsocket = zmq_socket (get_zmq_context(), ZMQ_REP);
    if (zmqsocket == NULL) {
        int zerrno = zmq_errno();
        g_info("[%s:%d] new rep socket error: %d %s", __func__, __LINE__, zerrno, zerrno == 0 ? "None" : zmq_strerror(zerrno));
        return NULL;
    }
    GZmqSource * source = g_zmq_source_new(zmqsocket, max_mem_size, context);
    if (source != NULL) {
        source->type = EASY_ZMQ_REP;
        _pollfdOn(source);
    }
    return source;
}

GZmqSource * g_zmq_source_pub_new(GMainContext *context) {
    void *zmqsocket = zmq_socket (get_zmq_context(), ZMQ_PUB);
    if (zmqsocket == NULL) {
        int zerrno = zmq_errno();
        g_info("[%s:%d] new pub socket error: %d %s", __func__, __LINE__, zerrno, zerrno == 0 ? "None" : zmq_strerror(zerrno));
        return NULL;
    }
    GZmqSource * source = g_zmq_source_new(zmqsocket, 0, context);
    if (source != NULL) {
        source->type = EASY_ZMQ_PUB;
    }
    return source;
}

GZmqSource * g_zmq_source_sub_new(size_t max_mem_size, GMainContext *context) {
    void *zmqsocket = zmq_socket (get_zmq_context(), ZMQ_SUB);
    if (zmqsocket == NULL) {
        int zerrno = zmq_errno();
        g_info("[%s:%d] new pub socket error: %d %s", __func__, __LINE__, zerrno, zerrno == 0 ? "None" : zmq_strerror(zerrno));
        return NULL;
    }
    GZmqSource * source = g_zmq_source_new(zmqsocket, max_mem_size, context);
    if (source != NULL) {
        source->type = EASY_ZMQ_SUB;
        _pollfdOn(source);
    }
    return source;
}

GZmqSource * g_zmq_source_pair_new(size_t max_mem_size, GMainContext *context) {
    void *zmqsocket = zmq_socket (get_zmq_context(), ZMQ_PAIR);
    if (zmqsocket == NULL) {
        int zerrno = zmq_errno();
        g_info("[%s:%d] new pair socket error: %d %s", __func__, __LINE__, zerrno, zerrno == 0 ? "None" : zmq_strerror(zerrno));
        return NULL;
    }
    GZmqSource * source = g_zmq_source_new(zmqsocket, max_mem_size, context);
    if (source != NULL) {
        source->type = EASY_ZMQ_PAIR;
        _pollfdOn(source);
    }
    return source;
}

//

static void my_zmq_free_fn(void *data, void *hint) {
    if ((data != NULL) && (hint != NULL)) {
        GDestroyNotify destroy_data = (GDestroyNotify)hint;
        destroy_data(data);
    }
}

// RESPONSE SOCKET

typedef struct rep_callback_data {
    GZmqSourceFunc callback;
    gpointer user_data;
    GDestroyNotify notify;
} rep_callback_data_t;

static void rep_callback_data_free(gpointer data) {
    if (data != NULL) {
        g_debug("[%s:%d] free all (%p)", __func__, __LINE__, (void*)data);
        g_slice_free (rep_callback_data_t, data);
    } else {
        g_warning("[%s:%d] no data to free", __func__, __LINE__);
    }
}

static gboolean rep_callback(GZmqSource *zmq_source, const guint8 *buf, int len, gboolean more, gpointer user_data) {
    g_assert(user_data != NULL);
    rep_callback_data_t * const pdata = user_data;
    g_debug("[%s:%d] (%p)", __func__, __LINE__, (void*)zmq_source);

    _pollfdOff(zmq_source);

    g_zmq_source_ref(zmq_source);    
    if (G_LIKELY(pdata->callback != NULL)) {
        pdata->callback(zmq_source, buf, len, more, pdata->user_data);
    }
    if ((pdata->notify != NULL) && (pdata->user_data != NULL)) {
        pdata->notify(pdata->user_data);
    }
    pdata->notify = NULL;
    pdata->user_data = NULL;
    g_zmq_source_unref(zmq_source);
    
    return G_SOURCE_CONTINUE;
}

gboolean recv_zmq_rep(GZmqSource *zmq_source, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify) {
    g_assert(zmq_source != NULL);
    g_debug("[%s:%d] register for responce receiving [%p]", __func__, __LINE__, (void*)zmq_source);

    rep_callback_data_t * const pdata = g_slice_new0(rep_callback_data_t);
    pdata->user_data = user_data;
    pdata->notify = notify;
    pdata->callback = cb;

    g_source_set_callback((GSource *) zmq_source, (GSourceFunc) rep_callback, pdata, rep_callback_data_free);

    return TRUE;
}

gboolean send_zmq_rep(GZmqSource *zmq_source, const guint8 *buf, int len) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    g_assert(buf != NULL);
    g_assert(len > 0);
    g_assert(zmq_source->type == EASY_ZMQ_REP);

    _pollfdOn(zmq_source);

    zmq_msg_t message;
    assert_errno(zmq_msg_init_size(&message, len) == 0, "error %s[%d] on zmq_msg_init_size", clean_errno(), zmq_errno());
    memcpy(zmq_msg_data(&message), buf, len);
    int r = zmq_msg_send(&message, zmq_source->zmq_owned_socket, ZMQ_SEND_FLAGS);

    if (G_UNLIKELY(r == -1)) {
        _pollfdOff(zmq_source);
        g_debug("[%s:%d] send resp message error (%d,%s) [%p,%p]", __func__, __LINE__, zmq_errno(), clean_errno(), (void*)zmq_source, (void*)zmq_source->zmq_owned_socket);
        assert_errno(zmq_msg_close(&message) == 0, "error %s[%d] on zmq_msg_close", clean_errno(), zmq_errno());
        return FALSE;
    } else {
        g_debug("[%s:%d] send resp message ok [%p,%p]", __func__, __LINE__, (void*)zmq_source, (void*)zmq_source->zmq_owned_socket);
        return TRUE;
    }
}

gboolean send_zmq_resp_take(GZmqSource *zmq_source, guint8 *buf, int len, GDestroyNotify destroy_data) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    g_assert(buf != NULL);
    g_assert(len > 0);
    g_assert(zmq_source->type == EASY_ZMQ_REP);

    _pollfdOn(zmq_source);

    zmq_msg_t message;
    assert_errno(zmq_msg_init_data(&message, buf, len, my_zmq_free_fn, destroy_data) == 0, "error %s[%d] on zmq_msg_init_data", clean_errno(), errno);
    memcpy(zmq_msg_data(&message), buf, len);
    int r = zmq_msg_send(&message, zmq_source->zmq_owned_socket, ZMQ_SEND_FLAGS);

    if (G_UNLIKELY(r == -1)) {
        _pollfdOff(zmq_source);
        g_debug("[%s:%d] send resp message error (%d,%s) [%p,%p]", __func__, __LINE__, zmq_errno(), clean_errno(), (void*)zmq_source, (void*)zmq_source->zmq_owned_socket);
        assert_errno(zmq_msg_close(&message) == 0, "error %s[%d] on zmq_msg_close", clean_errno(), zmq_errno());
        return FALSE;
    } else {
        g_debug("[%s:%d] send resp message ok [%p,%p]", __func__, __LINE__, (void*)zmq_source, (void*)zmq_source->zmq_owned_socket);
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
    guint timeoutms;
} req_callback_data_t;

static void req_callback_data_free(gpointer data) {
    if (data != NULL) {
        req_callback_data_t * const pdata = data;
        if (G_LIKELY((pdata->notify != NULL) && (pdata->user_data != NULL))) {
            g_debug("[%s:%d] free user data", __func__, __LINE__);
            pdata->notify(pdata->user_data);
        }
        pdata->user_data = NULL;
        pdata->notify = NULL;
        g_debug("[%s:%d] free all (%p)", __func__, __LINE__, (void*)data);
        g_slice_free (req_callback_data_t, data);
    } else {
        g_warning("[%s:%d] no data to free", __func__, __LINE__);
    }
}

static gboolean req_callback(GZmqSource *zmq_source, const guint8 *buf, int len, gboolean more, gpointer user_data) {
    g_assert(user_data != NULL);
    req_callback_data_t * const pdata = user_data;
    g_debug("[%s:%d] (%p)", __func__, __LINE__, (void*)zmq_source);
    if ((pdata->timeout_handler == 0) && (pdata->timeoutms > 0U)) {
        g_warning("[%s:%d] timeout already manage so skeep this (%p)", __func__, __LINE__, (void*)zmq_source);
    } else {
        if (pdata->timeout_handler > 0U) {
            g_source_remove(pdata->timeout_handler);
            pdata->timeout_handler = 0U;
        }
        _pollfdOff(zmq_source);
        g_zmq_source_ref(zmq_source);
        pdata->timeoutms = 0U;
        GZmqSourceFunc callback = pdata->callback;
        GDestroyNotify notify2 = pdata->notify;
        void * const user_data2 = pdata->user_data;
        GZmqSource * const zmq_source = pdata->zmq_source;
        pdata->notify = NULL;
        pdata->user_data = NULL;
        g_debug("[%s:%d] change callback", __func__, __LINE__);
        g_source_set_callback((GSource *) zmq_source, (GSourceFunc) fake_callback, NULL, NULL);
        if (G_LIKELY(callback != NULL)) {
            callback(zmq_source, buf, len, more, user_data2);
        }
        if (G_LIKELY((notify2 != NULL) && (user_data2 != NULL))) {
            g_debug("[%s:%d] free user data", __func__, __LINE__);
            notify2(user_data2);
        }
        g_zmq_source_unref(zmq_source);
        
    }
    return G_SOURCE_CONTINUE;
}

gboolean req_timeout_callback(gpointer user_data) {
    g_assert(user_data != NULL);
    req_callback_data_t * const pdata = (req_callback_data_t*) user_data;
    pdata->timeout_handler = 0U;
    pdata->timeoutms = 0U;
    void * const user_data2 = pdata->user_data;
    GDestroyNotify notify2 = pdata->notify;
    GZmqSource * const zmq_source = pdata->zmq_source;
    GZmqSourceFunc callback = pdata->callback;
    pdata->notify = NULL;
    pdata->user_data = NULL;
    g_debug("[%s:%d] (%p)", __func__, __LINE__, (void*)zmq_source);
    _pollfdOff(zmq_source);
    g_zmq_source_ref(zmq_source);
    g_debug("[%s:%d] change callback", __func__, __LINE__);
    g_source_set_callback((GSource *) zmq_source, (GSourceFunc) fake_callback, NULL, NULL); // this call means destroy user_data
    if (G_LIKELY(callback != NULL)) {
        // so all the used parameter is saved before
        // use it
        g_debug("[%s:%d] call callback", __func__, __LINE__);
        callback(zmq_source, NULL, 0, FALSE, user_data2);
    }
    if (G_LIKELY((notify2 != NULL) && (user_data2 != NULL))) {
        g_debug("[%s:%d] free user data", __func__, __LINE__);
        notify2(user_data2);
    }
    g_zmq_source_unref(zmq_source);

    return G_SOURCE_REMOVE;
}

gboolean send_zmq_req(GZmqSource *zmq_source, guint timeoutms, const guint8 *buf, int len, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify) {
    g_assert(zmq_source != NULL);
    g_assert(cb != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    g_assert(buf != NULL);
    g_assert(len > 0);
    g_assert(zmq_source->type == EASY_ZMQ_REQ);

    gboolean ret = FALSE;

    req_callback_data_t * const pdata = g_slice_new0(req_callback_data_t);
    pdata->zmq_source = zmq_source;
    pdata->user_data = user_data;
    pdata->notify = notify;
    pdata->callback = cb;
    pdata->timeout_handler = 0U;
    pdata->timeoutms = 0U;

    g_source_set_callback((GSource *) zmq_source, (GSourceFunc) req_callback, pdata, req_callback_data_free);

    if (timeoutms > 0U) {
        int timeoutms = timeoutms;
        if (_zmq_setsockopt(zmq_source->zmq_owned_socket, ZMQ_SNDTIMEO, &timeoutms, sizeof(timeoutms)) != 0) {
            g_info("(%p) error %s[%d] on zmq_setsockopt", (void*)zmq_source, clean_errno(), zmq_errno());
            g_source_set_callback((GSource *) zmq_source, (GSourceFunc) fake_callback, NULL, NULL);
            return FALSE;
        }
        int req_correlate = 1;
        if (_zmq_setsockopt(zmq_source->zmq_owned_socket, ZMQ_REQ_CORRELATE, &req_correlate, sizeof(req_correlate)) != 0) {
            g_info("(%p) error %s[%d] on zmq_setsockopt", (void*)zmq_source, clean_errno(), zmq_errno());
            //g_source_set_callback((GSource *) zmq_source, (GSourceFunc) fake_callback, NULL, NULL);
            //return FALSE;
        }
        int req_relaxed = 1;
        if (_zmq_setsockopt(zmq_source->zmq_owned_socket, ZMQ_REQ_RELAXED, &req_relaxed, sizeof(req_relaxed)) != 0) {
            g_info("(%p) error %s[%d] on zmq_setsockopt", (void*)zmq_source, clean_errno(), zmq_errno());
            //g_source_set_callback((GSource *) zmq_source, (GSourceFunc) fake_callback, NULL, NULL);
            //return FALSE;
        }
    } else {
        int timeoutms = -1;
        if (_zmq_setsockopt(zmq_source->zmq_owned_socket, ZMQ_SNDTIMEO, &timeoutms, sizeof(timeoutms)) != 0) {
            g_info("(%p) error %s[%d] on zmq_setsockopt", (void*)zmq_source, clean_errno(), zmq_errno());
            g_source_set_callback((GSource *) zmq_source, (GSourceFunc) fake_callback, NULL, NULL);
            return FALSE;
        }
        int req_correlate = 0;
        if (_zmq_setsockopt(zmq_source->zmq_owned_socket, ZMQ_REQ_CORRELATE, &req_correlate, sizeof(req_correlate)) != 0) {
            g_info("(%p) error %s[%d] on zmq_setsockopt", (void*)zmq_source, clean_errno(), zmq_errno());
            //g_source_set_callback((GSource *) zmq_source, (GSourceFunc) fake_callback, NULL, NULL);
            //return FALSE;
        }
        int req_relaxed = 0;
        if (_zmq_setsockopt(zmq_source->zmq_owned_socket, ZMQ_REQ_RELAXED, &req_relaxed, sizeof(req_relaxed)) != 0) {
            g_info("(%p) error %s[%d] on zmq_setsockopt", (void*)zmq_source, clean_errno(), zmq_errno());
            //g_source_set_callback((GSource *) zmq_source, (GSourceFunc) fake_callback, NULL, NULL);
            //return FALSE;
        }
    }

    _pollfdOn(zmq_source);

    zmq_msg_t message;
    assert_errno(zmq_msg_init_size(&message, len) == 0, "error %s[%d] on zmq_msg_init_size", clean_errno(), zmq_errno());
    memcpy(zmq_msg_data(&message), buf, len);
    int r = zmq_msg_send(&message, zmq_source->zmq_owned_socket, ZMQ_SEND_FLAGS);

    if (G_UNLIKELY(r == -1)) {
        g_info("[%s:%d] send req message error (%d, %s) [%p,%p]", __func__, __LINE__, zmq_errno(), clean_errno(), (void*)zmq_source, (void*)zmq_source->zmq_owned_socket);
        assert_errno(zmq_msg_close(&message) == 0, "error %s[%d] on zmq_msg_close", clean_errno(), zmq_errno());
        g_source_set_callback((GSource *) zmq_source, (GSourceFunc) fake_callback, NULL, NULL);
    } else {
        g_debug("[%s:%d] send req message ok [%p,%p]", __func__, __LINE__, (void*)zmq_source, (void*)zmq_source->zmq_owned_socket);
        // add timeout before call error callback
        if (timeoutms > 0U) {
            pdata->timeout_handler = g_timeout_add(timeoutms, req_timeout_callback, pdata);
            if (pdata->timeout_handler > 0U) {
                pdata->timeoutms = timeoutms;
            }
        }
        ret = TRUE;
    }

    return ret;
}

gboolean send_zmq_req_take(GZmqSource *zmq_source, guint timeoutms, guint8 *buf, int len, GDestroyNotify destroy_data, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    g_assert(buf != NULL);
    g_assert(cb != NULL);
    g_assert(len > 0);
    g_assert(zmq_source->type == EASY_ZMQ_REQ);

    gboolean ret = FALSE;

    req_callback_data_t * const pdata = g_slice_new0(req_callback_data_t);
    pdata->zmq_source = zmq_source;
    pdata->user_data = user_data;
    pdata->notify = notify;
    pdata->callback = cb;
    pdata->timeout_handler = 0U;
    pdata->timeoutms = 0U;

    g_source_set_callback((GSource *) zmq_source, (GSourceFunc) req_callback, pdata, req_callback_data_free);

    if (timeoutms > 0U) {
        int timeoutms = timeoutms;
        if (_zmq_setsockopt(zmq_source->zmq_owned_socket, ZMQ_SNDTIMEO, &timeoutms, sizeof(timeoutms)) != 0) {
            g_info("(%p) error %s[%d] on zmq_setsockopt", (void*)zmq_source, clean_errno(), zmq_errno());
            g_source_set_callback((GSource *) zmq_source, (GSourceFunc) fake_callback, NULL, NULL);
            return FALSE;
        }
        int req_correlate = 1;
        if (_zmq_setsockopt(zmq_source->zmq_owned_socket, ZMQ_REQ_CORRELATE, &req_correlate, sizeof(req_correlate)) != 0) {
            g_info("(%p) error %s[%d] on zmq_setsockopt", (void*)zmq_source, clean_errno(), zmq_errno());
            //g_source_set_callback((GSource *) zmq_source, (GSourceFunc) fake_callback, NULL, NULL);
            //return FALSE;
        }
        int req_relaxed = 1;
        if (_zmq_setsockopt(zmq_source->zmq_owned_socket, ZMQ_REQ_RELAXED, &req_relaxed, sizeof(req_relaxed)) != 0) {
            g_info("(%p) error %s[%d] on zmq_setsockopt", (void*)zmq_source, clean_errno(), zmq_errno());
            //g_source_set_callback((GSource *) zmq_source, (GSourceFunc) fake_callback, NULL, NULL);
            //return FALSE;
        }
    } else {
        int timeoutms = -1;
        if (_zmq_setsockopt(zmq_source->zmq_owned_socket, ZMQ_SNDTIMEO, &timeoutms, sizeof(timeoutms)) != 0) {
            g_info("(%p) error %s[%d] on zmq_setsockopt", (void*)zmq_source, clean_errno(), zmq_errno());
            g_source_set_callback((GSource *) zmq_source, (GSourceFunc) fake_callback, NULL, NULL);
            return FALSE;
        }
        int req_correlate = 0;
        if (_zmq_setsockopt(zmq_source->zmq_owned_socket, ZMQ_REQ_CORRELATE, &req_correlate, sizeof(req_correlate)) != 0) {
            g_info("(%p) error %s[%d] on zmq_setsockopt", (void*)zmq_source, clean_errno(), zmq_errno());
            //g_source_set_callback((GSource *) zmq_source, (GSourceFunc) fake_callback, NULL, NULL);
            //return FALSE;
        }
        int req_relaxed = 0;
        if (_zmq_setsockopt(zmq_source->zmq_owned_socket, ZMQ_REQ_RELAXED, &req_relaxed, sizeof(req_relaxed)) != 0) {
            g_info("(%p) error %s[%d] on zmq_setsockopt", (void*)zmq_source, clean_errno(), zmq_errno());
            //g_source_set_callback((GSource *) zmq_source, (GSourceFunc) fake_callback, NULL, NULL);
            //return FALSE;
        }
    }

    zmq_msg_t message;
    assert_errno(zmq_msg_init_data(&message, buf, len, my_zmq_free_fn, destroy_data) == 0, "error %s[%d] on zmq_msg_init_data", clean_errno(), zmq_errno());
    int r = zmq_msg_send(&message, zmq_source->zmq_owned_socket, ZMQ_SEND_FLAGS);

    if (G_UNLIKELY(r == -1)) {
        g_info("[%s:%d] send req message error (%d, %s) [%p,%p]", __func__, __LINE__, zmq_errno(), clean_errno(), (void*)zmq_source, (void*)zmq_source->zmq_owned_socket);
        assert_errno(zmq_msg_close(&message) == 0, "error %s[%d] on zmq_msg_close", clean_errno(), zmq_errno());
        g_source_set_callback((GSource *) zmq_source, (GSourceFunc) fake_callback, NULL, NULL);
    } else {
        g_debug("[%s:%d] send req message ok [%p,%p]", __func__, __LINE__, (void*)zmq_source, (void*)zmq_source->zmq_owned_socket);
        // add timeout before call error callback
        if (timeoutms > 0U) {
            pdata->timeout_handler = g_timeout_add(timeoutms, req_timeout_callback, pdata);
            if (pdata->timeout_handler > 0U) {
                pdata->timeoutms = timeoutms;
            }
        }
        ret = TRUE;
    }

    return ret;
}

// SUB SOCKET

gboolean subscribe_zmq_sub(GZmqSource *zmq_source, const guint8 *filter, int len, GError **error) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    g_assert(zmq_source->type == EASY_ZMQ_SUB);

    if (filter == NULL) {
        filter = (const guint8 *)"";
        len = 0;
    }

    int rc = _zmq_setsockopt(zmq_source->zmq_owned_socket, ZMQ_SUBSCRIBE, filter, len);
    if (rc != 0) {
        int zerrno = zmq_errno();
        g_info("[%s:%d] subscribe filter error: %d %s", __func__, __LINE__, zerrno, zerrno == 0 ? "None" : zmq_strerror(zerrno));
        g_set_error (error, EASY_ZMQ_ERROR, zerrno,
		     "easyzmq errno: %d %s during connect",
		     zerrno, zmq_strerror(zerrno));
        return FALSE;
    }

    g_debug("[%s:%d] set filter receiving", __func__, __LINE__);
    return TRUE;
}

gboolean unsubscribe_zmq_sub(GZmqSource *zmq_source, const guint8 *filter, int len, GError **error) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    g_assert(zmq_source->type == EASY_ZMQ_SUB);

    if (filter == NULL) {
        filter = (const guint8 *)"";
        len = 0;
    }

    int rc = _zmq_setsockopt(zmq_source->zmq_owned_socket, ZMQ_UNSUBSCRIBE, filter, len);
    if (rc != 0) {
        int zerrno = zmq_errno();
        g_info("[%s:%d] subscribe filter error: %d %s", __func__, __LINE__, zerrno, zerrno == 0 ? "None" : zmq_strerror(zerrno));
        g_set_error (error, EASY_ZMQ_ERROR, zerrno,
		     "easyzmq errno: %d %s during connect",
		     zerrno, zmq_strerror(zerrno));
        return FALSE;
    }

    g_debug("[%s:%d] unset filter receiving", __func__, __LINE__);
    return TRUE;
}


gboolean recv_zmq_sub(GZmqSource *zmq_source, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    g_assert(zmq_source->type == EASY_ZMQ_SUB);

    g_debug("[%s:%d] register for subscriber receiving", __func__, __LINE__);
    if (cb == NULL) {
        cb = fake_callback;
    }

    g_source_set_callback((GSource *) zmq_source, (GSourceFunc) cb, user_data, notify);

    return TRUE;
}

// PUB SOCKET

gboolean send_zmq_pub(GZmqSource *zmq_source, const guint8 *buf, int len) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    g_assert(buf != NULL);
    g_assert(len > 0);
    g_assert(zmq_source->type == EASY_ZMQ_PUB);

    gboolean ret = FALSE;

    zmq_msg_t message;
    assert_errno(zmq_msg_init_size(&message, len) == 0, "error %s[%d] on zmq_msg_init_size", clean_errno(), zmq_errno());
    memcpy(zmq_msg_data(&message), buf, len);
    int r = zmq_msg_send(&message, zmq_source->zmq_owned_socket, ZMQ_SEND_FLAGS);

    if (r == -1) {
        g_debug("[%s:%d] send pub message error (%d, %s)", __func__, __LINE__, zmq_errno(), clean_errno());
        assert_errno(zmq_msg_close(&message) == 0, "error %s[%d] on zmq_msg_close", clean_errno(), zmq_errno());
    } else {
        g_debug("[%s:%d] send pub message ok", __func__, __LINE__);
        ret = TRUE;
    }

    return ret;
}

gboolean send_zmq_pub_take(GZmqSource *zmq_source, guint8 *buf, int len, GDestroyNotify destroy_data) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    g_assert(buf != NULL);
    g_assert(len > 0);
    g_assert(zmq_source->type == EASY_ZMQ_PUB);

    gboolean ret = FALSE;

    zmq_msg_t message;
    assert_errno(zmq_msg_init_data(&message, buf, len, my_zmq_free_fn, destroy_data) == 0, "error %s[%d] on zmq_msg_init_data", clean_errno(), zmq_errno());
    memcpy(zmq_msg_data(&message), buf, len);
    int r = zmq_msg_send(&message, zmq_source->zmq_owned_socket, ZMQ_SEND_FLAGS);

    if (r == -1) {
        g_debug("[%s:%d] send pub message error (%d, %s)", __func__, __LINE__, zmq_errno(), clean_errno());
        assert_errno(zmq_msg_close(&message) == 0, "error %s[%d] on zmq_msg_close", clean_errno(), zmq_errno());
    } else {
        g_debug("[%s:%d] send pub message ok", __func__, __LINE__);
        ret = TRUE;
    }

    return ret;
}

// PAIR SOCKET

gboolean recv_zmq_pair(GZmqSource *zmq_source, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->type == EASY_ZMQ_PAIR);
    g_debug("[%s:%d] register for pair receiving [%p]", __func__, __LINE__, (void*)zmq_source);
    if (cb == NULL) {
        cb = fake_callback;
    }
    g_source_set_callback((GSource *) zmq_source, (GSourceFunc) cb, user_data, notify);

    return TRUE;
}

gboolean send_zmq_pair(GZmqSource *zmq_source, const guint8 *buf, int len) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    g_assert(buf != NULL);
    g_assert(len > 0);
    g_assert(zmq_source->type == EASY_ZMQ_PAIR);

    zmq_msg_t message;
    assert_errno(zmq_msg_init_size(&message, len) == 0, "error %s[%d] on zmq_msg_init_size", clean_errno(), zmq_errno());
    memcpy(zmq_msg_data(&message), buf, len);
    int r = zmq_msg_send(&message, zmq_source->zmq_owned_socket, ZMQ_SEND_FLAGS);

    if (G_UNLIKELY(r == -1)) {
        g_debug("[%s:%d] send resp message error (%d,%s) [%p,%p]", __func__, __LINE__, zmq_errno(), clean_errno(), (void*)zmq_source, (void*)zmq_source->zmq_owned_socket);
        assert_errno(zmq_msg_close(&message) == 0, "error %s[%d] on zmq_msg_close", clean_errno(), zmq_errno());
        return FALSE;
    } else {
        g_debug("[%s:%d] send resp message ok [%p,%p]", __func__, __LINE__, (void*)zmq_source, (void*)zmq_source->zmq_owned_socket);
        return TRUE;
    }
}

gboolean send_zmq_pair_take(GZmqSource *zmq_source, guint8 *buf, int len, GDestroyNotify destroy_data) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    g_assert(buf != NULL);
    g_assert(len > 0);

    zmq_msg_t message;
    assert_errno(zmq_msg_init_data(&message, buf, len, my_zmq_free_fn, destroy_data) == 0, "error %s[%d] on zmq_msg_init_data", clean_errno(), errno);
    memcpy(zmq_msg_data(&message), buf, len);
    int r = zmq_msg_send(&message, zmq_source->zmq_owned_socket, ZMQ_SEND_FLAGS);

    if (G_UNLIKELY(r == -1)) {
        g_debug("[%s:%d] send resp message error (%d,%s) [%p,%p]", __func__, __LINE__, zmq_errno(), clean_errno(), (void*)zmq_source, (void*)zmq_source->zmq_owned_socket);
        assert_errno(zmq_msg_close(&message) == 0, "error %s[%d] on zmq_msg_close", clean_errno(), zmq_errno());
        return FALSE;
    } else {
        g_debug("[%s:%d] send resp message ok [%p,%p]", __func__, __LINE__, (void*)zmq_source, (void*)zmq_source->zmq_owned_socket);
        return TRUE;
    }
}

// Monitor

typedef struct monitor_callback_data {
    int state;
    guint32 value;
    easyzmq_socket_t event;
    GZmqMonitorFunc callback;
} monitor_callback_data_t;

static void monitor_callback_data_free(gpointer data) {
    if (data != NULL) {
        g_slice_free (monitor_callback_data_t, data);
    } else {
        g_warning("[%s:%d] no data to free", __func__, __LINE__);
    }
}

static gboolean get_easyzmq_monitor_event(const guint8 *buf, int len , easyzmq_socket_t *event, guint32 *value) {
    // The first frame contains an event number (16 bits), and an event value (32 bits) that provides additional data according to the event number. 
    if (len == 6) {
        struct {
            guint16 event;
            guint32 value;
        } __attribute__((packed)) decoded_buffer;
        g_assert(len == sizeof(decoded_buffer));

        memcpy(&decoded_buffer, buf, sizeof(decoded_buffer));
        if (value) *value = decoded_buffer.value;
        if (event) {
            if (decoded_buffer.event & ZMQ_EVENT_CONNECTED) {
                *event = EASYZMQ_EVENT_CONNECTED;
            } else if (decoded_buffer.event & ZMQ_EVENT_CONNECT_DELAYED) {
                *event = EASYZMQ_EVENT_CONNECT_DELAYED;
            } else if (decoded_buffer.event & ZMQ_EVENT_CONNECT_RETRIED) {
                *event = EASYZMQ_EVENT_CONNECT_RETRIED;
            } else if (decoded_buffer.event & ZMQ_EVENT_LISTENING) {
                *event = ZMQ_EVENT_LISTENING;
            } else if (decoded_buffer.event & ZMQ_EVENT_BIND_FAILED) {
                *event = EASYZMQ_EVENT_BIND_FAILED;
            } else if (decoded_buffer.event & ZMQ_EVENT_ACCEPTED) {
                *event = EASYZMQ_EVENT_ACCEPTED;
            } else if (decoded_buffer.event & ZMQ_EVENT_ACCEPT_FAILED) {
                *event = EASYZMQ_EVENT_ACCEPT_FAILED;
            } else if (decoded_buffer.event & ZMQ_EVENT_CLOSED) {
                *event = EASYZMQ_EVENT_CLOSED;
            } else if (decoded_buffer.event & ZMQ_EVENT_CLOSE_FAILED) {
                *event = EASYZMQ_EVENT_CLOSE_FAILED;
            } else if (decoded_buffer.event & ZMQ_EVENT_DISCONNECTED) {
                *event = EASYZMQ_EVENT_DISCONNECTED;
            } else if (decoded_buffer.event & ZMQ_EVENT_MONITOR_STOPPED) {
                *event = EASYZMQ_EVENT_MONITOR_STOPPED;
            } else {
                *event = EASYZMQ_EVENT_UNKNOWN;
            }
        }

        return TRUE;
    } else {
        return FALSE;
    }
}

static gboolean monitor_callback (GZmqSource *zmq_source, const guint8 *buf, int len, G_GNUC_UNUSED gboolean more, G_GNUC_UNUSED gpointer user_data) {
    g_assert(zmq_source != NULL);
    g_assert(user_data != NULL);
    g_assert(buf != NULL);
    g_assert(len > 0);

    monitor_callback_data_t *pdata = (monitor_callback_data_t *) user_data;
    
    if (more) {
        g_assert(pdata->state == 0);
        if (get_easyzmq_monitor_event(buf, len, &(pdata->event), &(pdata->value))) {
            pdata->state = 1;
        } else {
            g_error("size error");
        }
    } else {
        g_assert(pdata->state == 1);
        #pragma GCC diagnostic push
        #pragma GCC diagnostic ignored "-Wenum-compare"
        if ((pdata->callback) && 
            (pdata->event != EASYZMQ_EVENT_CONNECT_DELAYED) &&
            (pdata->event != EASYZMQ_EVENT_CONNECT_RETRIED) &&
            (pdata->event != EASYZMQ_EVENT_CLOSED)) {
            pdata->callback(zmq_source, pdata->event, pdata->value, buf, len);
        }
        #pragma GCC diagnostic pop
        pdata->state = 0;
    }
    
    return TRUE;
}

#define ZMQ_EVENT_ATTACHED (ZMQ_EVENT_ALL)

GZmqSource * get_zmq_socket_monitor(GZmqSource *zmq_source, GZmqMonitorFunc cb) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    if (zmq_source->zmq_source_monitor == NULL) {
        guint64 unique = get_unique();
        gchar *address = g_strdup_printf("inproc://%" G_GUINT64_FORMAT, unique);
        g_assert(address != NULL);

        if (zmq_socket_monitor (zmq_source->zmq_owned_socket, address, ZMQ_EVENT_ATTACHED) == 0) {
            zmq_source->zmq_source_monitor = g_zmq_source_pair_new(0, g_source_get_context ((GSource *) zmq_source));
            g_assert(zmq_source->zmq_owned_socket != NULL);

            monitor_callback_data_t * const pdata = g_slice_new0(monitor_callback_data_t);
            g_assert(pdata != NULL);
            pdata->state = 0;
            pdata->callback = cb;

            recv_zmq_pair(zmq_source->zmq_source_monitor, monitor_callback, pdata, monitor_callback_data_free);

            if (!g_easyzmq_connect (zmq_source->zmq_source_monitor, address, NULL)) {
                g_zmq_source_unref(zmq_source->zmq_source_monitor);
                zmq_source->zmq_source_monitor = NULL;
            } else {
                g_zmq_source_ref(zmq_source->zmq_source_monitor);
            }
            
        } else {
            g_warning("error creating monitor socket: %s", clean_errno());
        }
        g_free(address); address = NULL;

        return zmq_source->zmq_source_monitor;
    } else {
        g_assert(cb == NULL);
        g_zmq_source_ref(zmq_source->zmq_source_monitor);
        return zmq_source->zmq_source_monitor;
    }
}
