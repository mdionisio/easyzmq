
#include "easyzmq.h"

#include <glib.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>
#include <zmq.h>

// uncomment next line if you need extra debug
// #define EASYZMQ_DBG

#ifdef EASYZMQ_DBG
#define DBG(x, ...) g_debug(x, ##__VA_ARGS__)
#else
#define DBG(x, ...)
#endif

#define clean_zmq_strerror(x) ((x) == 0 ? "None" : zmq_strerror(x))
#define clean_zmq_errno() clean_zmq_strerror(zmq_errno())

#define assert_errno(x, message, ...)                                       \
    do {                                                                    \
        if (G_UNLIKELY(!(x))) {                                             \
            g_error("%s:%d - " message, __FILE__, __LINE__, ##__VA_ARGS__); \
        }                                                                   \
    } while (0)

#define check_errno(x, message, ...)                                       \
    do {                                                                   \
        if (G_UNLIKELY(!(x))) {                                            \
            g_info("%s:%d - " message, __FILE__, __LINE__, ##__VA_ARGS__); \
        }                                                                  \
    } while (0)

#define ISNULL(x) (G_LIKELY(NULL == (x)))
#define ISNOTNULL(x) (G_LIKELY(NULL != (x)))

static int _zmq_setsockopt(void *socket, int option_name, const void *option_value, size_t option_len) {
    g_assert(socket != NULL);
    g_assert(option_value != NULL);
    int retval = 0;
    do {
        retval = zmq_setsockopt(socket, option_name, option_value, option_len);
    } while ((retval != 0) && (EINTR == zmq_errno()));
    return retval;
}

static int _zmq_getsockopt(void *socket, int option_name, void *option_value, size_t *option_len) {
    g_assert(socket != NULL);
    g_assert(option_value != NULL);
    g_assert(option_len != NULL);
    int retval = 0;
    do {
        retval = zmq_getsockopt(socket, option_name, option_value, option_len);
    } while ((0 != retval) && (EINTR == zmq_errno()));
    return retval;
}

static int _zmq_recv(void *socket, void *buf, size_t len, int flags) {
    g_assert(socket != NULL);
    g_assert(buf != NULL);
    int retval = 0;
    do {
        retval = zmq_recv(socket, buf, len, flags);
    } while ((-1 == retval) && (EINTR == zmq_errno()));
    return retval;
}

static int _zmq_recvmsg(void *socket, zmq_msg_t *msg, int flags) {
    g_assert(socket != NULL);
    g_assert(msg != NULL);
    int retval = 0;
    do {
        retval = zmq_recvmsg(socket, msg, flags);
    } while ((-1 == retval) && (EINTR == zmq_errno()));
    return retval;
}

static int _zmq_msg_send(zmq_msg_t *msg, void *socket, int flags) {
    g_assert(socket != NULL);
    g_assert(msg != NULL);
    int retval = 0;
    do {
        retval = zmq_msg_send(msg, socket, flags);
    } while ((-1 == retval) && (EINTR == zmq_errno()));
    return retval;
}

static guint get_unique(void) {
    static gint counter = 0;
    return (guint)g_atomic_int_add(&counter, 1);
}

static void *zmq_context = NULL;

void easy_zmq_init(void) {
    if (ISNULL(zmq_context)) {
        zmq_context = zmq_ctx_new();
    } else {
        g_warning("easy_zmq_init already initilized");
    }
    g_assert(zmq_context != NULL);
}

void easy_zmq_deinit(void) {
    if (ISNOTNULL(zmq_context)) {
        void *const zmq_old_context = zmq_context;
        zmq_context = NULL;
        zmq_ctx_term(zmq_old_context);
    } else {
        g_warning("easy_zmq_deinit already destroyed");
    }
}

void *get_zmq_context(void) {
    g_assert(zmq_context != NULL);
    return zmq_context;
}

/**
 * easyzmq_error_quark:
 *
 * Gets the EasyZMQ Error Quark.
 *
 * Returns: a #GQuark.
 **/
G_DEFINE_QUARK(easyzmq - error - quark, easyzmq_error)

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
};


static void _pollfdOn(GZmqSource *source);
static void _pollfdOff(GZmqSource *source);

static gboolean _g_zmq_source_prepare(GSource *source, gint *timeout) {
    g_assert(source != NULL);

    GZmqSource *const self = (GZmqSource *)source;

    if (ISNOTNULL(self->zmq_owned_socket)) {
        gboolean ret = FALSE;
        if (timeout) {
            *timeout = -1;
        }
        int zevents = 0;
        size_t zevents_len = sizeof(zevents);
        if (_zmq_getsockopt(self->zmq_owned_socket, ZMQ_EVENTS, &zevents, &zevents_len) != 0) {
            // ETERM: context terminated while the source is still alive
            assert_errno(zmq_errno() == ETERM, "error %s[%d] on zmq_getsockopt", clean_zmq_errno(), zmq_errno());
            return FALSE;
        }

        ret = (zevents & (ZMQ_POLLIN | ZMQ_POLLERR));

        return ret;
    } else {
        g_info("not possible to preparate because not socket initialized");
        return FALSE;
    }
}

static gboolean _g_zmq_source_check(GSource *source) {
    g_assert(source != NULL);
    GZmqSource *const self = (GZmqSource *)source;

    if (ISNOTNULL(self->zmq_owned_socket)) {
        gboolean ret = FALSE;
        int zevents = 0;
        size_t zevents_len = sizeof(zevents);
        if (_zmq_getsockopt(self->zmq_owned_socket, ZMQ_EVENTS, &zevents, &zevents_len) != 0) {
            assert_errno(zmq_errno() == ETERM, "error %s[%d] on zmq_getsockopt", clean_zmq_errno(), zmq_errno());
            return FALSE;
        }

        ret = (zevents & (ZMQ_POLLIN | ZMQ_POLLERR));

        return ret;
    } else {
        return FALSE;
    }
}

static gboolean _g_zmq_source_dispatch(GSource *source, GSourceFunc callback, gpointer user_data) {
    g_assert(source != NULL);
    GZmqSource *const self = (GZmqSource *)source;
    GZmqSourceFunc const zcallback = (GZmqSourceFunc)callback;

    for (;;) {
        if (g_source_is_destroyed(source)) {
            // the source has been destroyed from inside a callback
            return G_SOURCE_REMOVE;
        }
        if (ISNOTNULL(self->zmq_owned_socket)) { // valid socket (using zeromq)
            int zevents = 0;
            int done = 0;
            size_t zevents_len = sizeof(zevents);
            if (_zmq_getsockopt(self->zmq_owned_socket, ZMQ_EVENTS, &zevents, &zevents_len) != 0) {
                const int zerrno = zmq_errno();
                if (ETERM != zerrno) {
                    g_error("errno get_socketopt ZMQ_EVENT: %d %s", zerrno, clean_zmq_strerror(zerrno));
                }
                return G_SOURCE_REMOVE;
            }

            if (zevents & ZMQ_POLLIN) {
                done++;

                if (ISNOTNULL(self->msg_receive)) {
                    int rc = _zmq_recv(self->zmq_owned_socket, self->msg_receive, self->size_receive, 0 /* WAIT BECAUSE I ALREADY CHECK THAT THERE IS DATA */);
                    gboolean ret = FALSE;
                    gboolean last_part = FALSE;
                    if ((rc < 0) || (((size_t)rc) > self->size_receive)) {
                        g_info("error receiving zmq message (%d,%zu): %s (%d)", rc, self->size_receive, clean_zmq_errno(), zmq_errno());
                        if (ISNOTNULL(zcallback)) {
                            ret = zcallback(self, NULL, 0, FALSE, user_data);
                        } else {
                            g_info("receive error when there is no subscription on it");
                            ret = G_SOURCE_REMOVE;
                        }
                    } else if (ISNOTNULL(zcallback)) {
                        int more = 0;
                        size_t more_len = sizeof(more);
                        assert_errno(_zmq_getsockopt(self->zmq_owned_socket, ZMQ_RCVMORE, &more, &more_len) == 0, "error %s[%d] on zmq_getsockopt", clean_zmq_errno(), zmq_errno());
                        DBG("data recevied -> call callback (buffer: %p)", self->msg_receive);
                        last_part = (more == 0);
                        ret = zcallback(self,
                                        (const guint8 *)self->msg_receive, 
                                        rc, (more != 0) ? TRUE : FALSE, 
                                        user_data);
                        DBG("data recevied -> call callback return:%d (buffer: %p)", (int)ret, self->msg_receive);
                    } else {
                        g_info("receive message when there is no subscription on it");

                        ret = G_SOURCE_REMOVE;
                    }
                    if (ret == G_SOURCE_REMOVE) {
                        return G_SOURCE_REMOVE;
                    }
                    if ((self->type == EASY_ZMQ_REQ) && last_part) {
                        // a REQ callback is valid only for one answer: the user
                        // callback could have sent a new request, so the next
                        // message has to be dispatched with the new callback
                        return G_SOURCE_CONTINUE;
                    }
                } else {
                    zmq_msg_t msg;
                    if (zmq_msg_init(&msg) == 0) {
                        DBG("data recevied create msg (msg: %p)", &msg);
                        const int size = _zmq_recvmsg(self->zmq_owned_socket, &msg, 0 /* WAIT BECAUSE I ALREADY CHECK THAT THERE IS DATA */);
                        gboolean ret = FALSE;
                        gboolean last_part = FALSE;
                        if (size < 0) {
                            g_info("error receiving zmq message: %s (%d)", clean_zmq_errno(), zmq_errno());
                            if (ISNOTNULL(zcallback)) {
                                ret = zcallback(self, NULL, 0, FALSE, user_data);
                            } else {
                                g_info("receive error when there is no subscription on it");
                                ret = G_SOURCE_REMOVE;
                            }
                        } else {
                            last_part = !zmq_msg_more(&msg);
                            if (ISNOTNULL(zcallback)) {
                                DBG("data recevied -> call callback (msg: %p)", &msg);
                                ret = zcallback(self, 
                                    (const guint8 *)zmq_msg_data(&msg), 
                                    (int)zmq_msg_size(&msg), // I think that this cast is always valid and also the variable size can be use
                                    zmq_msg_more(&msg), user_data);
                                DBG("data recevied -> call callback return :%d (msg: %p)", (int)ret, &msg);
                            } else {
                                g_info("receive message when there is no subscription on it");
                                ret = G_SOURCE_REMOVE;
                            }
                        }
                        DBG("data recevied clear msg (msg: %p)", &msg);
                        assert_errno(zmq_msg_close(&msg) == 0, "error %s[%d] on zmq_msg_close", clean_zmq_errno(), zmq_errno());

                        if (ret == G_SOURCE_REMOVE) {
                            return G_SOURCE_REMOVE;
                        }
                        if ((self->type == EASY_ZMQ_REQ) && last_part) {
                            // see comment above
                            return G_SOURCE_CONTINUE;
                        }
                    } else {
                        g_info("no memory to receive any message");
                        return G_SOURCE_REMOVE;
                    }
                }
            }

            if (zevents & ZMQ_POLLOUT) {
                done++;
                break;
            }

            if (zevents & ZMQ_POLLERR) {
                g_warning("ZMQ_POLLERR has not to happens");
                if (ISNOTNULL(zcallback)) {
                    gboolean ret = zcallback(self, NULL, 0, FALSE, user_data);
                    if (ret == G_SOURCE_REMOVE) {
                        return G_SOURCE_REMOVE;
                    }
                } else {
                    g_info("receive error when there is no subscription on it");
                    return G_SOURCE_REMOVE;
                }
                done++;
            }

            if (done == 0) {
                break;
            }
        } else { // not valid socket
            return G_SOURCE_REMOVE;
        }
    } // end for(;;)

    return G_SOURCE_CONTINUE;
}

static void _g_easyzmq_destroy_userdata(GZmqSource *self) {
    g_assert(self != NULL);
    if (ISNOTNULL(self->user_data)) {
        if (ISNOTNULL(self->destroy_user_data)) {
            self->destroy_user_data(self->user_data);
        }
        self->user_data = NULL;
    }
}

static void _g_zmq_source_finalize(GSource *source) {
    g_assert(source != NULL);
    GZmqSource *const self = (GZmqSource *)source;

    _pollfdOff(self);
    self->fd = -1;

    g_free(self->msg_receive);
    self->msg_receive = NULL;

    g_zmq_source_unref(self->zmq_source_monitor);
    self->zmq_source_monitor = NULL;
    _g_easyzmq_destroy_userdata(self);

    if (ISNOTNULL(self->zmq_owned_socket)) {
        int linger = 0;
        check_errno(_zmq_setsockopt(self->zmq_owned_socket, ZMQ_LINGER, &linger, sizeof(linger)) == 0, "error %s[%d] on zmq_setsockopt", clean_zmq_errno(), zmq_errno());
        assert_errno(zmq_close(self->zmq_owned_socket) == 0, "error %s[%d] on zmq_close", clean_zmq_errno(), zmq_errno());
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

static GSourceFuncs _g_zmq_source_funcs = {.prepare = _g_zmq_source_prepare, .check = _g_zmq_source_check, .dispatch = _g_zmq_source_dispatch, .finalize = _g_zmq_source_finalize};

static gboolean fake_callback(G_GNUC_UNUSED GZmqSource *zmq_source, G_GNUC_UNUSED const guint8 *buf, G_GNUC_UNUSED int len, G_GNUC_UNUSED gboolean more, G_GNUC_UNUSED gpointer user_data) {
    g_assert(zmq_source != NULL);
    g_warning("probably data received out of order");
    // g_assert_not_reached();
    return G_SOURCE_CONTINUE;
}

GZmqSource *g_zmq_source_ref(GZmqSource *self) {
    g_return_val_if_fail(self != NULL, NULL);
    g_warn_if_fail(self->ref_count != 0);

    self->ref_count++;

    g_source_ref((GSource *)self);
    return self;
}

void g_zmq_source_unref(GZmqSource *self) {
    if (ISNOTNULL(self)) {
        if (self->ref_count > 0) {
            self->ref_count--;
            if (self->ref_count == 0) {
                g_source_destroy((GSource *)self);
            } else {
                g_source_unref((GSource *)self);
            }
        } else {
            g_warn_if_reached();
        }
    }
}

void g_zmq_source_destroy(GZmqSource *self) {
    g_return_if_fail(self != NULL);

    g_source_destroy((GSource *)self);
}

gboolean g_easyzmq_bind(GZmqSource *source, const char *addr, GError **error) {
    g_assert(source != NULL);
    g_assert(addr != NULL);
    g_assert(source->zmq_owned_socket != NULL);
    int rc = zmq_bind(source->zmq_owned_socket, addr);
    if (rc != 0) {
        const int zerrno = zmq_errno();
        g_info("bind error: %d %s", zerrno, clean_zmq_strerror(zerrno));
        g_set_error(error, EASY_ZMQ_ERROR, zerrno, "easyzmq errno: %d %s during bind", zerrno, clean_zmq_strerror(zerrno));
        return FALSE;
    }
    return TRUE;
}

gboolean g_easyzmq_unbind(GZmqSource *source, const char *addr, GError **error) {
    g_assert(source != NULL);
    g_assert(addr != NULL);
    g_assert(source->zmq_owned_socket != NULL);
    int rc = zmq_unbind(source->zmq_owned_socket, addr);
    if (rc != 0) {
        const int zerrno = zmq_errno();
        g_info("unbind error: %d %s", zerrno, clean_zmq_strerror(zerrno));
        g_set_error(error, EASY_ZMQ_ERROR, zerrno, "easyzmq errno: %d %s during unbind", zerrno, clean_zmq_strerror(zerrno));
        return FALSE;
    }
    return TRUE;
}

gboolean g_easyzmq_connect(GZmqSource *source, const char *addr, GError **error) {
    g_assert(source != NULL);
    g_assert(addr != NULL);
    g_assert(source->zmq_owned_socket != NULL);
    int rc = zmq_connect(source->zmq_owned_socket, addr);
    if (rc != 0) {
        const int zerrno = zmq_errno();
        g_info("connect error: %d %s", zerrno, clean_zmq_strerror(zerrno));
        g_set_error(error, EASY_ZMQ_ERROR, zerrno, "easyzmq errno: %d %s during connect", zerrno, clean_zmq_strerror(zerrno));
        return FALSE;
    }
    return TRUE;
}

gboolean g_easyzmq_disconnect(GZmqSource *source, const char *addr, GError **error) {
    g_assert(source != NULL);
    g_assert(addr != NULL);
    g_assert(source->zmq_owned_socket != NULL);
    int rc = zmq_disconnect(source->zmq_owned_socket, addr);
    if (rc != 0) {
        const int zerrno = zmq_errno();
        g_info("disconnect error: %d %s", zerrno, clean_zmq_strerror(zerrno));
        g_set_error(error, EASY_ZMQ_ERROR, zerrno, "easyzmq errno: %d %s during disconnect", zerrno, clean_zmq_strerror(zerrno));
        return FALSE;
    }
    return TRUE;
}

static void _pollfdOn(GZmqSource *source) {
    g_assert(source != NULL);
    g_assert(source->fd >= 0);

    if (ISNOTNULL(source->fdtag)) {
        return; // already polled
    }

    source->fdtag = g_source_add_unix_fd((GSource *)source, source->fd, G_IO_IN | G_IO_PRI | G_IO_ERR | G_IO_HUP | G_IO_NVAL);

    g_assert(source->fdtag != NULL);
}

static void _pollfdOff(GZmqSource *source) {
    g_assert(source != NULL);

    if (ISNOTNULL(source->fdtag)) {
        g_source_remove_unix_fd((GSource *)source, source->fdtag);
        source->fdtag = NULL;
    }
}

static easyzmq_socket_t _easyzmq_type_from_zmq(int zmq_type) {
    switch (zmq_type) {
    case ZMQ_PUB:
        return EASY_ZMQ_PUB;
    case ZMQ_SUB:
        return EASY_ZMQ_SUB;
    case ZMQ_REQ:
        return EASY_ZMQ_REQ;
    case ZMQ_REP:
        return EASY_ZMQ_REP;
    case ZMQ_PAIR:
        return EASY_ZMQ_PAIR;
    case ZMQ_ROUTER:
        return EASY_ZMQ_ROUTER;
    default:
        return EASY_ZMQ_UNKNOWN;
    }
}

// take the ownership of l_zmq_socket (closed also in case of error)
static GZmqSource *g_zmq_source_new(void *l_zmq_socket, size_t max_mem_size, GMainContext *l_context) {
    g_assert(l_zmq_socket != NULL);
    if (ISNULL(l_context)) {
        l_context = g_main_context_default();
    }
    g_assert(l_context != NULL);

    int zmq_type = -1;
    size_t zmq_type_len = sizeof(zmq_type);
    if (_zmq_getsockopt(l_zmq_socket, ZMQ_TYPE, &zmq_type, &zmq_type_len) != 0) {
        g_info("GZmqSource create error: %s", clean_zmq_errno());
        assert_errno(zmq_close(l_zmq_socket) == 0, "error %s[%d] on zmq_close", clean_zmq_errno(), zmq_errno());
        return NULL;
    }
    const easyzmq_socket_t type = _easyzmq_type_from_zmq(zmq_type);
    if (type == EASY_ZMQ_UNKNOWN) {
        g_info("GZmqSource create error: zmq socket type %d not supported", zmq_type);
        assert_errno(zmq_close(l_zmq_socket) == 0, "error %s[%d] on zmq_close", clean_zmq_errno(), zmq_errno());
        return NULL;
    }

    GZmqSource *source = (GZmqSource *)g_source_new(&_g_zmq_source_funcs, sizeof(GZmqSource));
    g_assert(source != NULL);

    source->zmq_owned_socket = l_zmq_socket;
    source->type = type;
    source->send_flags = ZMQ_DONTWAIT; // never block the main loop
    source->fdtag = NULL;
    source->zmq_source_monitor = NULL;
    source->fd = -1;
    source->ref_count = 1;

    if ((max_mem_size > 0) && (type != EASY_ZMQ_PUB)) {
        int64_t max_msg_size = (int64_t)max_mem_size;
        if (_zmq_setsockopt(l_zmq_socket, ZMQ_MAXMSGSIZE, &max_msg_size, sizeof(max_msg_size)) != 0) {
            g_warning("GZmqSource error setting max message size %s", clean_zmq_errno());
        }
        source->msg_receive = g_malloc0(max_mem_size);
        source->size_receive = max_mem_size;
    } else {
        source->msg_receive = NULL;
        source->size_receive = 0;
    }

    int fd = -1;
    size_t fd_len = sizeof(fd);
    if ((_zmq_getsockopt(source->zmq_owned_socket, ZMQ_FD, &fd, &fd_len) != 0) || (fd < 0)) {
        g_info("GZmqSource create error: %s", clean_zmq_errno());
        g_source_unref((GSource *)source); // not attached: finalize close the socket
        return NULL;
    }
    source->fd = fd;

    g_source_set_callback((GSource *)source, (GSourceFunc)(void *)fake_callback, NULL, NULL);
    g_source_attach((GSource *)source, l_context);
    g_source_unref((GSource *)source);

    switch (type) {
    case EASY_ZMQ_REQ: // polled only when a request is pending
    case EASY_ZMQ_PUB: // never receive
        break;
    default:
        _pollfdOn(source);
        break;
    }

    return source;
}

GZmqSource *g_zmq_source_new_from_socket(void *zmq_socket, size_t max_mem_size, GMainContext *context) {
    g_return_val_if_fail(zmq_socket != NULL, NULL);
    return g_zmq_source_new(zmq_socket, max_mem_size, context);
}

static GZmqSource *_g_zmq_source_new_type(int zmq_type, size_t max_mem_size, GMainContext *context) {
    void *const zmqsocket = zmq_socket(get_zmq_context(), zmq_type);
    if (ISNOTNULL(zmqsocket)) {
        return g_zmq_source_new(zmqsocket, max_mem_size, context);
    } else {
        const int zerrno = zmq_errno();
        g_info("new socket (type %d) error: %d %s", zmq_type, zerrno, clean_zmq_strerror(zerrno));
        return NULL;
    }
}

GZmqSource *g_zmq_source_req_new(size_t max_mem_size, GMainContext *context) {
    return _g_zmq_source_new_type(ZMQ_REQ, max_mem_size, context);
}

GZmqSource *g_zmq_source_rep_new(size_t max_mem_size, GMainContext *context) {
    return _g_zmq_source_new_type(ZMQ_REP, max_mem_size, context);
}

GZmqSource *g_zmq_source_router_new(size_t max_mem_size, GMainContext *context) {
    return _g_zmq_source_new_type(ZMQ_ROUTER, max_mem_size, context);
}

GZmqSource *g_zmq_source_pub_new(GMainContext *context) {
    return _g_zmq_source_new_type(ZMQ_PUB, 0, context);
}

GZmqSource *g_zmq_source_sub_new(size_t max_mem_size, GMainContext *context) {
    return _g_zmq_source_new_type(ZMQ_SUB, max_mem_size, context);
}

GZmqSource *g_zmq_source_pair_new(size_t max_mem_size, GMainContext *context) {
    return _g_zmq_source_new_type(ZMQ_PAIR, max_mem_size, context);
}

easyzmq_socket_t g_easyzmq_get_socket_type(GZmqSource *self) {
    g_return_val_if_fail(self != NULL, EASY_ZMQ_UNKNOWN);
    return self->type;
}

void g_easyzmq_set_blocking_send(GZmqSource *self, gboolean blocking) {
    g_return_if_fail(self != NULL);
    self->send_flags = blocking ? 0 : ZMQ_DONTWAIT;
}

gboolean g_easyzmq_get_blocking_send(GZmqSource *self) {
    g_return_val_if_fail(self != NULL, FALSE);
    return (self->send_flags & ZMQ_DONTWAIT) ? FALSE : TRUE;
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
    g_assert(data != NULL);
    rep_callback_data_t *const pdata = (rep_callback_data_t *)data;
    // user_data is valid for all the messages received, so it is released
    // only when the callback is replaced or the source is destroyed
    if (ISNOTNULL(pdata->notify) && ISNOTNULL(pdata->user_data)) {
        pdata->notify(pdata->user_data);
    }
    g_slice_free(rep_callback_data_t, pdata);
}

static gboolean rep_callback(GZmqSource *zmq_source, const guint8 *buf, int len, gboolean more, gpointer user_data) {
    g_assert(user_data != NULL);
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->type == EASY_ZMQ_REP);
    rep_callback_data_t *const pdata = (rep_callback_data_t *)user_data;

    if (!more) {
        _pollfdOff(zmq_source);
    }

    g_zmq_source_ref(zmq_source);
    if (ISNOTNULL(pdata->callback)) {
        pdata->callback(zmq_source, buf, len, more, pdata->user_data);
    }
    g_zmq_source_unref(zmq_source);

    return G_SOURCE_CONTINUE;
}

gboolean recv_zmq_rep(GZmqSource *zmq_source, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->type == EASY_ZMQ_REP);

    rep_callback_data_t *const pdata = g_slice_new0(rep_callback_data_t);
    pdata->user_data = user_data;
    pdata->notify = notify;
    pdata->callback = cb;

    g_source_set_callback((GSource *)zmq_source, (GSourceFunc)rep_callback, pdata, rep_callback_data_free);

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
    assert_errno(zmq_msg_init_size(&message, len) == 0, "error %s[%d] on zmq_msg_init_size", clean_zmq_errno(), zmq_errno());
    memcpy(zmq_msg_data(&message), buf, len);
    int r = _zmq_msg_send(&message, zmq_source->zmq_owned_socket, zmq_source->send_flags);

    if (G_UNLIKELY(r == -1)) {
        _pollfdOff(zmq_source);
        g_debug("send resp message error (%d,%s)", zmq_errno(), clean_zmq_errno());
        assert_errno(zmq_msg_close(&message) == 0, "error %s[%d] on zmq_msg_close", clean_zmq_errno(), zmq_errno());
        return FALSE;
    } else {
        return TRUE;
    }
}

gboolean send_zmq_rep_take(GZmqSource *zmq_source, guint8 *buf, int len, GDestroyNotify destroy_data) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    g_assert(buf != NULL);
    g_assert(len > 0);
    g_assert(zmq_source->type == EASY_ZMQ_REP);

    _pollfdOn(zmq_source);

    zmq_msg_t message;
    assert_errno(zmq_msg_init_data(&message, buf, len, my_zmq_free_fn, destroy_data) == 0, "error %s[%d] on zmq_msg_init_data", clean_zmq_errno(), zmq_errno());
    int r = _zmq_msg_send(&message, zmq_source->zmq_owned_socket, zmq_source->send_flags);

    if (G_UNLIKELY(r == -1)) {
        _pollfdOff(zmq_source);
        g_debug("send resp message error (%d,%s)", zmq_errno(), clean_zmq_errno());
        assert_errno(zmq_msg_close(&message) == 0, "error %s[%d] on zmq_msg_close", clean_zmq_errno(), zmq_errno());
        return FALSE;
    } else {
        return TRUE;
    }
}

// ROUTER SOCKET

typedef struct idenity_list_element {
    guint8 *buf;
    int len;
} idenity_list_element_t;

typedef struct router_callback_data {
    GZmqSourceIdentityFunc callback;
    gpointer user_data;
    GList *identity_list;
    gboolean identity_completed;
    GDestroyNotify notify;
} router_callback_data_t;

static void router_callback_data_free(gpointer data) {
    g_assert(data != NULL);
    router_callback_data_t *const pdata = (router_callback_data_t *)data;
    if (ISNOTNULL(pdata->identity_list)) {
        destroy_identity(pdata->identity_list);
        pdata->identity_list = NULL;
    }
    if (ISNOTNULL(pdata->notify) && ISNOTNULL(pdata->user_data)) {
        pdata->notify(pdata->user_data);
    }
    g_slice_free(router_callback_data_t, pdata);
}

static gboolean router_callback(GZmqSource *zmq_source, const guint8 *buf, int len, gboolean more, gpointer user_data) {
    g_assert(user_data != NULL);
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->type == EASY_ZMQ_ROUTER);
    g_assert(len >= 0);
    router_callback_data_t *const pdata = (router_callback_data_t *)user_data;

    if (pdata->identity_completed == FALSE) {
        if ((!more) || (buf == NULL)) {
            // malformed envelope (peer is not a REQ socket) or receive error:
            // never trust remote data, just drop the message
            g_info("router: message without envelope delimiter, discarded");
            if (ISNOTNULL(pdata->identity_list)) {
                destroy_identity(pdata->identity_list);
                pdata->identity_list = NULL;
            }
        } else if (len > 0) {
            idenity_list_element_t *element = g_slice_new0(idenity_list_element_t);
#if GLIB_CHECK_VERSION(2, 68, 0)
            element->buf = g_memdup2(buf, (gsize)len);
#else
            element->buf = g_memdup(buf, (guint)len);
#endif
            element->len = len;
            // keep the frames in receive order: they have to be sent back in the same order
            pdata->identity_list = g_list_append(pdata->identity_list, element);
        } else {
            pdata->identity_completed = TRUE;
        }
    } else {
        g_zmq_source_ref(zmq_source);
        void *const identity = (void *)pdata->identity_list;
        if (!more) {
            // the ownership of the identity goes to the user
            // (send_zmq_router* or destroy_identity release it)
            pdata->identity_list = NULL;
            pdata->identity_completed = FALSE;
        }
        if (ISNOTNULL(pdata->callback)) {
            pdata->callback(zmq_source, identity, buf, len, more, pdata->user_data);
        } else if ((!more) && ISNOTNULL(identity)) {
            destroy_identity(identity);
        }
        g_zmq_source_unref(zmq_source);
    }

    return G_SOURCE_CONTINUE;
}

gboolean recv_zmq_router(GZmqSource *zmq_source, GZmqSourceIdentityFunc cb, gpointer user_data, GDestroyNotify notify) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->type == EASY_ZMQ_ROUTER);

    router_callback_data_t *const pdata = g_slice_new0(router_callback_data_t);
    pdata->user_data = user_data;
    pdata->notify = notify;
    pdata->callback = cb;
    pdata->identity_list = NULL;
    pdata->identity_completed = FALSE;

    g_source_set_callback((GSource *)zmq_source, (GSourceFunc)router_callback, pdata, router_callback_data_free);

    return TRUE;
}

void destroy_identity(void *pdata) {
    g_assert(pdata != NULL);
    GList *identity = (GList *)pdata;
    g_assert(identity != NULL);

    while (ISNOTNULL(identity)) {
        GList * const identity_element = identity;
        g_assert(identity_element->data != NULL);
        identity = g_list_remove_link(identity, identity_element);
        idenity_list_element_t * const elem = (idenity_list_element_t *)(identity_element->data);
        g_assert(elem != NULL);
        g_free(elem->buf);
        g_slice_free(idenity_list_element_t, identity_element->data);
        g_list_free(identity_element);
    }
}

// send the routing envelope (identity frames + empty delimiter) and release the identity
static gboolean _send_router_envelope(GZmqSource *zmq_source, GList *identity) {
    gboolean ok = TRUE;

    while (ISNOTNULL(identity)) {
        GList *const identity_element = identity;
        g_assert(identity_element->data != NULL);
        identity = g_list_remove_link(identity, identity_element);
        idenity_list_element_t *const elem = (idenity_list_element_t *)(identity_element->data);
        if (ok) {
            zmq_msg_t message;
            // the buffer ownership goes to zmq
            assert_errno(zmq_msg_init_data(&message, elem->buf, elem->len, my_zmq_free_fn, (void *)g_free) == 0, "error %s[%d] on zmq_msg_init_data", clean_zmq_errno(), zmq_errno());
            if (G_UNLIKELY(_zmq_msg_send(&message, zmq_source->zmq_owned_socket, zmq_source->send_flags | ZMQ_SNDMORE) == -1)) {
                g_info("send router identity error (%d,%s)", zmq_errno(), clean_zmq_errno());
                assert_errno(zmq_msg_close(&message) == 0, "error %s[%d] on zmq_msg_close", clean_zmq_errno(), zmq_errno());
                ok = FALSE;
            }
        } else {
            g_free(elem->buf);
        }
        g_slice_free(idenity_list_element_t, elem);
        g_list_free(identity_element);
    }

    if (ok) {
        zmq_msg_t message;
        assert_errno(zmq_msg_init_size(&message, 0) == 0, "error %s[%d] on zmq_msg_init_size", clean_zmq_errno(), zmq_errno());
        if (G_UNLIKELY(_zmq_msg_send(&message, zmq_source->zmq_owned_socket, zmq_source->send_flags | ZMQ_SNDMORE) == -1)) {
            g_info("send router delimiter error (%d,%s)", zmq_errno(), clean_zmq_errno());
            assert_errno(zmq_msg_close(&message) == 0, "error %s[%d] on zmq_msg_close", clean_zmq_errno(), zmq_errno());
            ok = FALSE;
        }
    }

    return ok;
}

static gboolean _send_router_msg(GZmqSource *zmq_source, void *pdata, zmq_msg_t *message) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    g_assert(zmq_source->type == EASY_ZMQ_ROUTER);
    g_assert(pdata != NULL);

    if (G_UNLIKELY(!_send_router_envelope(zmq_source, (GList *)pdata))) {
        assert_errno(zmq_msg_close(message) == 0, "error %s[%d] on zmq_msg_close", clean_zmq_errno(), zmq_errno());
        return FALSE;
    }

    if (G_UNLIKELY(_zmq_msg_send(message, zmq_source->zmq_owned_socket, zmq_source->send_flags) == -1)) {
        g_info("send router message error (%d,%s)", zmq_errno(), clean_zmq_errno());
        assert_errno(zmq_msg_close(message) == 0, "error %s[%d] on zmq_msg_close", clean_zmq_errno(), zmq_errno());
        return FALSE;
    }
    return TRUE;
}

gboolean send_zmq_router(GZmqSource *zmq_source, void *pdata, const guint8 *buf, int len) {
    g_assert(buf != NULL);
    g_assert(len > 0);

    zmq_msg_t message;
    assert_errno(zmq_msg_init_size(&message, len) == 0, "error %s[%d] on zmq_msg_init_size", clean_zmq_errno(), zmq_errno());
    memcpy(zmq_msg_data(&message), buf, len);
    return _send_router_msg(zmq_source, pdata, &message);
}

gboolean send_zmq_router_take(GZmqSource *zmq_source, void *pdata, guint8 *buf, int len, GDestroyNotify destroy_data) {
    g_assert(buf != NULL);
    g_assert(len > 0);

    zmq_msg_t message;
    assert_errno(zmq_msg_init_data(&message, buf, len, my_zmq_free_fn, (void *)destroy_data) == 0, "error %s[%d] on zmq_msg_init_data", clean_zmq_errno(), zmq_errno());
    return _send_router_msg(zmq_source, pdata, &message);
}

// REQUEST SOCKET

typedef struct req_callback_data {
    gpointer user_data;
    GDestroyNotify notify;
    GZmqSourceFunc callback;
    GZmqSource *zmq_source;
    GSource *timeout_source; // owned reference, NULL when no timeout is pending
    gboolean timeout_expired;
} req_callback_data_t;

static void req_timeout_cancel(req_callback_data_t *pdata) {
    g_assert(pdata != NULL);
    if (ISNOTNULL(pdata->timeout_source)) {
        GSource *const timeout_source = pdata->timeout_source;
        pdata->timeout_source = NULL;
        g_source_destroy(timeout_source);
        g_source_unref(timeout_source);
    }
}

static void req_callback_data_free(gpointer data) {
    g_assert(data != NULL);

    req_callback_data_t *const pdata = (req_callback_data_t *)data;
    // the request could be dropped while the timeout is still pending
    // (new request or source destroyed): the timeout must not fire on freed memory
    req_timeout_cancel(pdata);
    if (G_LIKELY((pdata->notify != NULL) && (pdata->user_data != NULL))) {
        pdata->notify(pdata->user_data);
    }
    pdata->user_data = NULL;
    pdata->notify = NULL;
    g_slice_free(req_callback_data_t, pdata);
}

static gboolean req_callback(GZmqSource *zmq_source, const guint8 *buf, int len, gboolean more, gpointer user_data) {
    g_assert(user_data != NULL);
    g_assert(zmq_source != NULL);
    req_callback_data_t *const pdata = (req_callback_data_t *)user_data;
    if (pdata->timeout_expired) {
        g_warning("timeout already manage so skip this");
    } else {
        req_timeout_cancel(pdata);
        if (!more) {
            _pollfdOff(zmq_source);
        }

        g_zmq_source_ref(zmq_source);
        GZmqSourceFunc const callback = pdata->callback;
        GDestroyNotify notify2 = NULL;
        void *const user_data2 = pdata->user_data;
        if (!more) {
            notify2 = pdata->notify;
            pdata->notify = NULL;
            pdata->user_data = NULL;
            pdata->callback = NULL;
            // pdata is still valid until the end of the dispatch (GLib keeps a reference on the callback data)
            g_source_set_callback((GSource *)zmq_source, (GSourceFunc)(void *)fake_callback, NULL, NULL);
        }
        if (ISNOTNULL(callback)) {
            callback(zmq_source, buf, len, more, user_data2);
        }
        if (!more) {
            if (ISNOTNULL(notify2) && ISNOTNULL(user_data2)) {
                notify2(user_data2);
            }
        }
        g_zmq_source_unref(zmq_source);
    }
    return G_SOURCE_CONTINUE;
}

static gboolean req_timeout_callback(gpointer user_data) {
    g_assert(user_data != NULL);
    req_callback_data_t *const pdata = (req_callback_data_t *)user_data;
    g_assert(pdata->timeout_source != NULL);
    g_source_unref(pdata->timeout_source); // the source is destroyed returning G_SOURCE_REMOVE
    pdata->timeout_source = NULL;
    pdata->timeout_expired = TRUE;
    void *const user_data2 = pdata->user_data;
    GDestroyNotify const notify2 = pdata->notify;
    GZmqSource *const zmq_source = pdata->zmq_source;
    GZmqSourceFunc const callback = pdata->callback;
    pdata->notify = NULL;
    pdata->user_data = NULL;
    pdata->callback = NULL;
    _pollfdOff(zmq_source);
    g_zmq_source_ref(zmq_source);
    // this call destroy pdata so all the used parameter are saved before
    g_source_set_callback((GSource *)zmq_source, (GSourceFunc)(void *)fake_callback, NULL, NULL);
    if (ISNOTNULL(callback)) {
        callback(zmq_source, NULL, 0, FALSE, user_data2);
    }
    if (ISNOTNULL(notify2) && ISNOTNULL(user_data2)) {
        notify2(user_data2);
    }
    g_zmq_source_unref(zmq_source);

    return G_SOURCE_REMOVE;
}

static gboolean _req_configure(GZmqSource *zmq_source, guint timeoutms) {
    const gboolean with_timeout = (timeoutms > 0U) && (timeoutms < (guint)INT_MAX);
    // with timeout the socket has to accept a new request also if the answer never arrive
    const int sndtimeo = with_timeout ? (int)timeoutms : -1;
    const int relaxed = with_timeout ? 1 : 0;

    if (_zmq_setsockopt(zmq_source->zmq_owned_socket, ZMQ_SNDTIMEO, &sndtimeo, sizeof(sndtimeo)) != 0) {
        g_info("error %s[%d] on zmq_setsockopt ZMQ_SNDTIMEO", clean_zmq_errno(), zmq_errno());
        return FALSE;
    }
    if (_zmq_setsockopt(zmq_source->zmq_owned_socket, ZMQ_REQ_CORRELATE, &relaxed, sizeof(relaxed)) != 0) {
        g_info("error %s[%d] on zmq_setsockopt ZMQ_REQ_CORRELATE", clean_zmq_errno(), zmq_errno());
    }
    if (_zmq_setsockopt(zmq_source->zmq_owned_socket, ZMQ_REQ_RELAXED, &relaxed, sizeof(relaxed)) != 0) {
        g_info("error %s[%d] on zmq_setsockopt ZMQ_REQ_RELAXED", clean_zmq_errno(), zmq_errno());
    }
    return TRUE;
}

// send message (always consumed) and register the callback for the answer
static gboolean _send_zmq_req_msg(GZmqSource *zmq_source, guint timeoutms, zmq_msg_t *message, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify) {
    g_assert(zmq_source != NULL);
    g_assert(cb != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    g_assert(zmq_source->type == EASY_ZMQ_REQ);

    if ((!_req_configure(zmq_source, timeoutms)) || (_zmq_msg_send(message, zmq_source->zmq_owned_socket, zmq_source->send_flags) == -1)) {
        // the pending request (if any) is not touched
        g_info("send req message error (%d, %s)", zmq_errno(), clean_zmq_errno());
        assert_errno(zmq_msg_close(message) == 0, "error %s[%d] on zmq_msg_close", clean_zmq_errno(), zmq_errno());
        // user_data is always released (also in case of error)
        if (ISNOTNULL(notify) && ISNOTNULL(user_data)) {
            notify(user_data);
        }
        return FALSE;
    }

    // the answer is dispatched from the main loop so it is safe to register the callback after the send
    req_callback_data_t *const pdata = g_slice_new0(req_callback_data_t);
    pdata->zmq_source = zmq_source;
    pdata->user_data = user_data;
    pdata->notify = notify;
    pdata->callback = cb;
    pdata->timeout_source = NULL;
    pdata->timeout_expired = FALSE;

    // an old pending request (only possible with timeout/relaxed mode) is released here
    g_source_set_callback((GSource *)zmq_source, (GSourceFunc)(void *)req_callback, pdata, req_callback_data_free);

    if (timeoutms > 0U) {
        // the timeout has to run on the same context of the socket
        pdata->timeout_source = g_timeout_source_new(timeoutms);
        g_source_set_callback(pdata->timeout_source, req_timeout_callback, pdata, NULL);
        g_source_attach(pdata->timeout_source, g_source_get_context((GSource *)zmq_source));
    }

    _pollfdOn(zmq_source);

    return TRUE;
}

gboolean send_zmq_req(GZmqSource *zmq_source, guint timeoutms, const guint8 *buf, int len, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify) {
    g_assert(buf != NULL);
    g_assert(len > 0);

    zmq_msg_t message;
    assert_errno(zmq_msg_init_size(&message, len) == 0, "error %s[%d] on zmq_msg_init_size", clean_zmq_errno(), zmq_errno());
    memcpy(zmq_msg_data(&message), buf, len);

    return _send_zmq_req_msg(zmq_source, timeoutms, &message, cb, user_data, notify);
}

gboolean send_zmq_req_take(GZmqSource *zmq_source, guint timeoutms, guint8 *buf, int len, GDestroyNotify destroy_data, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify) {
    g_assert(buf != NULL);
    g_assert(len > 0);

    zmq_msg_t message;
    assert_errno(zmq_msg_init_data(&message, buf, len, my_zmq_free_fn, (void *)destroy_data) == 0, "error %s[%d] on zmq_msg_init_data", clean_zmq_errno(), zmq_errno());

    return _send_zmq_req_msg(zmq_source, timeoutms, &message, cb, user_data, notify);
}

// SUB SOCKET

gboolean subscribe_zmq_sub(GZmqSource *zmq_source, const guint8 *filter, int len, GError **error) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    g_assert(zmq_source->type == EASY_ZMQ_SUB);
    g_assert(len >= 0);

    if (filter == NULL) {
        filter = (const guint8 *)"";
        len = 0;
    }

    int rc = _zmq_setsockopt(zmq_source->zmq_owned_socket, ZMQ_SUBSCRIBE, filter, len);
    if (rc != 0) {
        int zerrno = zmq_errno();
        g_info("subscribe filter error: %d %s", zerrno, clean_zmq_strerror(zerrno));
        g_set_error(error, EASY_ZMQ_ERROR, zerrno, "easyzmq errno: %d %s during connect", zerrno, clean_zmq_strerror(zerrno));
        return FALSE;
    }

    return TRUE;
}

gboolean unsubscribe_zmq_sub(GZmqSource *zmq_source, const guint8 *filter, int len, GError **error) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    g_assert(zmq_source->type == EASY_ZMQ_SUB);
    g_assert(len >= 0);

    if (filter == NULL) {
        filter = (const guint8 *)"";
        len = 0;
    }

    int rc = _zmq_setsockopt(zmq_source->zmq_owned_socket, ZMQ_UNSUBSCRIBE, filter, len);
    if (rc != 0) {
        int zerrno = zmq_errno();
        g_info("subscribe filter error: %d %s", zerrno, clean_zmq_strerror(zerrno));
        g_set_error(error, EASY_ZMQ_ERROR, zerrno, "easyzmq errno: %d %s during connect", zerrno, clean_zmq_strerror(zerrno));
        return FALSE;
    }

    return TRUE;
}

gboolean recv_zmq_sub(GZmqSource *zmq_source, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    g_assert(zmq_source->type == EASY_ZMQ_SUB);

    if (ISNULL(cb)) {
        cb = fake_callback;
    }

    g_source_set_callback((GSource *)zmq_source, (GSourceFunc)cb, user_data, notify);

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
    assert_errno(zmq_msg_init_size(&message, len) == 0, "error %s[%d] on zmq_msg_init_size", clean_zmq_errno(), zmq_errno());
    memcpy(zmq_msg_data(&message), buf, len);
    int r = _zmq_msg_send(&message, zmq_source->zmq_owned_socket, zmq_source->send_flags);

    if (r == -1) {
        g_debug("send pub message error (%d, %s)", zmq_errno(), clean_zmq_errno());
        assert_errno(zmq_msg_close(&message) == 0, "error %s[%d] on zmq_msg_close", clean_zmq_errno(), zmq_errno());
    } else {
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
    assert_errno(zmq_msg_init_data(&message, buf, len, my_zmq_free_fn, destroy_data) == 0, "error %s[%d] on zmq_msg_init_data", clean_zmq_errno(), zmq_errno());
    int r = _zmq_msg_send(&message, zmq_source->zmq_owned_socket, zmq_source->send_flags);

    if (r == -1) {
        g_debug("send pub message error (%d, %s)", zmq_errno(), clean_zmq_errno());
        assert_errno(zmq_msg_close(&message) == 0, "error %s[%d] on zmq_msg_close", clean_zmq_errno(), zmq_errno());
    } else {
        ret = TRUE;
    }

    return ret;
}

// PAIR SOCKET

gboolean recv_zmq_pair(GZmqSource *zmq_source, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->type == EASY_ZMQ_PAIR);
    if (cb == NULL) {
        cb = fake_callback;
    }
    g_source_set_callback((GSource *)zmq_source, (GSourceFunc)cb, user_data, notify);

    return TRUE;
}

gboolean send_zmq_pair(GZmqSource *zmq_source, const guint8 *buf, int len) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    g_assert(buf != NULL);
    g_assert(len > 0);
    g_assert(zmq_source->type == EASY_ZMQ_PAIR);

    zmq_msg_t message;
    assert_errno(zmq_msg_init_size(&message, len) == 0, "error %s[%d] on zmq_msg_init_size", clean_zmq_errno(), zmq_errno());
    memcpy(zmq_msg_data(&message), buf, (size_t)len);
    int r = _zmq_msg_send(&message, zmq_source->zmq_owned_socket, zmq_source->send_flags);

    if (G_UNLIKELY(r == -1)) {
        g_debug("send resp message error (%d,%s)", zmq_errno(), clean_zmq_errno());
        assert_errno(zmq_msg_close(&message) == 0, "error %s[%d] on zmq_msg_close", clean_zmq_errno(), zmq_errno());
        return FALSE;
    } else {
        return TRUE;
    }
}

gboolean send_zmq_pair_take(GZmqSource *zmq_source, guint8 *buf, int len, GDestroyNotify destroy_data) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    g_assert(buf != NULL);
    g_assert(len > 0);
    g_assert(zmq_source->type == EASY_ZMQ_PAIR);

    zmq_msg_t message;
    assert_errno(zmq_msg_init_data(&message, buf, len, my_zmq_free_fn, destroy_data) == 0, "error %s[%d] on zmq_msg_init_data", clean_zmq_errno(), zmq_errno());
    int r = _zmq_msg_send(&message, zmq_source->zmq_owned_socket, zmq_source->send_flags);

    if (G_UNLIKELY(r == -1)) {
        g_debug("send resp message error (%d,%s)", zmq_errno(), clean_zmq_errno());
        assert_errno(zmq_msg_close(&message) == 0, "error %s[%d] on zmq_msg_close", clean_zmq_errno(), zmq_errno());
        return FALSE;
    } else {
        return TRUE;
    }
}

// Monitor

typedef struct monitor_callback_data {
    int state;
    guint32 value;
    easyzmq_monitor_event_t event;
    GZmqMonitorFunc callback;
} monitor_callback_data_t;

static void monitor_callback_data_free(gpointer data) {
    g_slice_free(monitor_callback_data_t, data);
}

static gboolean get_easyzmq_monitor_event(const guint8 *buf, int len, easyzmq_monitor_event_t *event, guint32 *value) {
    // The first frame contains an event number (16 bits), and an event value (32 bits) that provides additional data according to the event number.
    if (6 == len) {
        g_assert(buf != NULL);
        struct {
            guint16 event;
            guint32 value;
        } __attribute__((packed)) decoded_buffer;
        g_assert(len == sizeof(decoded_buffer));

        memcpy(&decoded_buffer, buf, sizeof(decoded_buffer));
        if (value) {
            *value = decoded_buffer.value;
        }
        if (event) {
            if (decoded_buffer.event & ZMQ_EVENT_CONNECTED) {
                *event = EASYZMQ_EVENT_CONNECTED;
            } else if (decoded_buffer.event & ZMQ_EVENT_CONNECT_DELAYED) {
                *event = EASYZMQ_EVENT_CONNECT_DELAYED;
            } else if (decoded_buffer.event & ZMQ_EVENT_CONNECT_RETRIED) {
                *event = EASYZMQ_EVENT_CONNECT_RETRIED;
            } else if (decoded_buffer.event & ZMQ_EVENT_LISTENING) {
                *event = EASYZMQ_EVENT_LISTENING;
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
        g_warning("monitor socket message not well formatted");
        return FALSE;
    }
}

static gboolean monitor_callback(GZmqSource *zmq_source, const guint8 *buf, int len, gboolean more, gpointer user_data) {
    g_assert(zmq_source != NULL);
    g_assert(user_data != NULL);

    monitor_callback_data_t *pdata = (monitor_callback_data_t *)user_data;

    if (ISNULL(buf)) {
        g_info("monitor receive error");
        pdata->state = 0;
        return TRUE;
    }

    if (more) {
        g_assert(pdata->state == 0);
        if (get_easyzmq_monitor_event(buf, len, &(pdata->event), &(pdata->value))) {
            pdata->state = 1;
        } else {
            g_error("size error");
        }
    } else {
        g_assert(pdata->state == 1);
        pdata->state = 0;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wenum-compare"
        if ((pdata->callback) && (pdata->event != EASYZMQ_EVENT_CONNECT_DELAYED) && (pdata->event != EASYZMQ_EVENT_CONNECT_RETRIED) && (pdata->event != EASYZMQ_EVENT_CLOSED)) {
            pdata->callback(zmq_source, pdata->event, pdata->value, buf, len);
        }
#pragma GCC diagnostic pop
    }

    return TRUE;
}

#define ZMQ_EVENT_ATTACHED (ZMQ_EVENT_ALL)

GZmqSource *get_zmq_socket_monitor(GZmqSource *zmq_source, GZmqMonitorFunc cb) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    if (ISNULL(zmq_source->zmq_source_monitor)) {
        guint unique = get_unique();
        gchar address[48];
        g_snprintf(address, G_N_ELEMENTS(address), "inproc://easyzmq-monitor-%u", unique);

        if (zmq_socket_monitor(zmq_source->zmq_owned_socket, address, ZMQ_EVENT_ATTACHED) == 0) {
            zmq_source->zmq_source_monitor = g_zmq_source_pair_new(0, g_source_get_context((GSource *)zmq_source));
            if (ISNULL(zmq_source->zmq_source_monitor)) {
                g_warning("error creating monitor pair socket");
                zmq_socket_monitor(zmq_source->zmq_owned_socket, NULL, 0);
                return NULL;
            }

            monitor_callback_data_t *const pdata = g_slice_new0(monitor_callback_data_t);
            g_assert(pdata != NULL);
            pdata->state = 0;
            pdata->callback = cb;

            recv_zmq_pair(zmq_source->zmq_source_monitor, monitor_callback, pdata, monitor_callback_data_free);

            if (!g_easyzmq_connect(zmq_source->zmq_source_monitor, address, NULL)) {
                g_zmq_source_unref(zmq_source->zmq_source_monitor);
                zmq_source->zmq_source_monitor = NULL;
            } else {
                g_zmq_source_ref(zmq_source->zmq_source_monitor);
            }

        } else {
            g_warning("error creating monitor socket: %s", clean_zmq_errno());
        }

        return zmq_source->zmq_source_monitor;
    } else {
        g_assert(cb == NULL);
        g_zmq_source_ref(zmq_source->zmq_source_monitor);
        return zmq_source->zmq_source_monitor;
    }
}
