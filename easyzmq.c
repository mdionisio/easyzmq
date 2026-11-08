
#include "easyzmq.h"
#include "easyzmq-private.h"

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
static gboolean zmq_context_owned = FALSE;
static guint zmq_context_users = 0U;
G_LOCK_DEFINE_STATIC(zmq_context);

static void _easy_zmq_init(void *external_context) {
    G_LOCK(zmq_context);
    if (zmq_context_users == 0U) {
        g_assert(zmq_context == NULL);
        if (ISNOTNULL(external_context)) {
            zmq_context = external_context;
            zmq_context_owned = FALSE;
        } else {
            zmq_context = zmq_ctx_new();
            zmq_context_owned = TRUE;
        }
        g_assert(zmq_context != NULL);
    } else if (ISNOTNULL(external_context) && (external_context != zmq_context)) {
        g_warning("easy_zmq_init_with_context: already initialized with a different context (ignored)");
    }
    zmq_context_users++;
    G_UNLOCK(zmq_context);
}

void easy_zmq_init(void) {
    _easy_zmq_init(NULL);
}

void easy_zmq_init_with_context(void *context) {
    g_return_if_fail(context != NULL);
    _easy_zmq_init(context);
}

void easy_zmq_deinit(void) {
    void *zmq_old_context = NULL;
    gboolean owned = FALSE;

    G_LOCK(zmq_context);
    if (zmq_context_users > 0U) {
        zmq_context_users--;
        if (zmq_context_users == 0U) {
            zmq_old_context = zmq_context;
            owned = zmq_context_owned;
            zmq_context = NULL;
            zmq_context_owned = FALSE;
        }
    } else {
        g_warning("easy_zmq_deinit without easy_zmq_init");
    }
    G_UNLOCK(zmq_context);

    if (ISNOTNULL(zmq_old_context) && owned) {
        // outside the lock: it blocks until all the sockets are closed
        zmq_ctx_term(zmq_old_context);
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

        // ZMQ_EVENTS is read also when paused: it resets the zmq fd notification
        ret = (!self->paused) && (zevents & (ZMQ_POLLIN | ZMQ_POLLERR));

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

        ret = (!self->paused) && (zevents & (ZMQ_POLLIN | ZMQ_POLLERR));

        return ret;
    } else {
        return FALSE;
    }
}

static gboolean _g_zmq_source_dispatch(GSource *source, GSourceFunc callback, gpointer user_data) {
    g_assert(source != NULL);
    GZmqSource *const self = (GZmqSource *)source;
    GZmqSourceFunc const zcallback = (GZmqSourceFunc)(void *)callback;

    for (;;) {
        if (g_source_is_destroyed(source)) {
            // the source has been destroyed from inside a callback
            return G_SOURCE_REMOVE;
        }
        if (self->paused) {
            // receive paused from inside a callback
            return G_SOURCE_CONTINUE;
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
                        self->current_buf = (const guint8 *)self->msg_receive;
                        self->current_len = rc;
                        ret = zcallback(self,
                                        (const guint8 *)self->msg_receive,
                                        rc, (more != 0) ? TRUE : FALSE,
                                        user_data);
                        self->current_buf = NULL;
                        self->current_len = 0;
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
                                self->current_msg = &msg;
                                ret = zcallback(self,
                                                (const guint8 *)zmq_msg_data(&msg),
                                                (int)zmq_msg_size(&msg), // I think that this cast is always valid and also the variable size can be use
                                                zmq_msg_more(&msg), user_data);
                                self->current_msg = NULL;
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

// The reference count is atomic: a reference can be taken and released from any thread
// (e.g. a worker thread that queues a send on the main loop).
GZmqSource *g_zmq_source_ref(GZmqSource *self) {
    g_return_val_if_fail(self != NULL, NULL);
    g_warn_if_fail(g_atomic_int_get(&self->ref_count) > 0);

    g_atomic_int_inc(&self->ref_count);

    g_source_ref((GSource *)self);
    return self;
}

void g_zmq_source_unref(GZmqSource *self) {
    if (ISNOTNULL(self)) {
        if (g_atomic_int_get(&self->ref_count) <= 0) {
            g_warn_if_reached();
        } else if (g_atomic_int_dec_and_test(&self->ref_count)) {
            // the last reference is the one of the main context
            g_source_destroy((GSource *)self);
        } else {
            g_source_unref((GSource *)self);
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
    case ZMQ_DEALER:
        return EASY_ZMQ_DEALER;
    case ZMQ_PUSH:
        return EASY_ZMQ_PUSH;
    case ZMQ_PULL:
        return EASY_ZMQ_PULL;
    case ZMQ_XPUB:
        return EASY_ZMQ_XPUB;
    case ZMQ_XSUB:
        return EASY_ZMQ_XSUB;
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

    if ((max_mem_size > 0) && (type != EASY_ZMQ_PUB) && (type != EASY_ZMQ_PUSH)) {
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
    case EASY_ZMQ_PUSH:
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

GZmqSource *g_zmq_source_dealer_new(size_t max_mem_size, GMainContext *context) {
    return _g_zmq_source_new_type(ZMQ_DEALER, max_mem_size, context);
}

GZmqSource *g_zmq_source_push_new(GMainContext *context) {
    return _g_zmq_source_new_type(ZMQ_PUSH, 0, context);
}

GZmqSource *g_zmq_source_pull_new(size_t max_mem_size, GMainContext *context) {
    return _g_zmq_source_new_type(ZMQ_PULL, max_mem_size, context);
}

GZmqSource *g_zmq_source_xpub_new(size_t max_mem_size, GMainContext *context) {
    return _g_zmq_source_new_type(ZMQ_XPUB, max_mem_size, context);
}

GZmqSource *g_zmq_source_xsub_new(size_t max_mem_size, GMainContext *context) {
    return _g_zmq_source_new_type(ZMQ_XSUB, max_mem_size, context);
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

void *g_easyzmq_get_socket(GZmqSource *self) {
    g_return_val_if_fail(self != NULL, NULL);
    return self->zmq_owned_socket;
}

static gboolean _set_option(GZmqSource *self, int option, const void *value, size_t size, GError **error) {
    g_return_val_if_fail(self != NULL, FALSE);
    g_return_val_if_fail(self->zmq_owned_socket != NULL, FALSE);
    if (_zmq_setsockopt(self->zmq_owned_socket, option, value, size) != 0) {
        const int zerrno = zmq_errno();
        g_set_error(error, EASY_ZMQ_ERROR, zerrno, "easyzmq errno: %d %s setting option %d", zerrno, clean_zmq_strerror(zerrno), option);
        return FALSE;
    }
    return TRUE;
}

static gboolean _get_option(GZmqSource *self, int option, void *value, size_t *size, GError **error) {
    g_return_val_if_fail(self != NULL, FALSE);
    g_return_val_if_fail(self->zmq_owned_socket != NULL, FALSE);
    if (_zmq_getsockopt(self->zmq_owned_socket, option, value, size) != 0) {
        const int zerrno = zmq_errno();
        g_set_error(error, EASY_ZMQ_ERROR, zerrno, "easyzmq errno: %d %s getting option %d", zerrno, clean_zmq_strerror(zerrno), option);
        return FALSE;
    }
    return TRUE;
}

gboolean g_easyzmq_set_option_int(GZmqSource *self, int option, int value, GError **error) {
    return _set_option(self, option, &value, sizeof(value), error);
}

gboolean g_easyzmq_get_option_int(GZmqSource *self, int option, int *value, GError **error) {
    g_return_val_if_fail(value != NULL, FALSE);
    size_t size = sizeof(*value);
    return _get_option(self, option, value, &size, error);
}

gboolean g_easyzmq_set_option_int64(GZmqSource *self, int option, gint64 value, GError **error) {
    int64_t v = (int64_t)value;
    return _set_option(self, option, &v, sizeof(v), error);
}

gboolean g_easyzmq_get_option_int64(GZmqSource *self, int option, gint64 *value, GError **error) {
    g_return_val_if_fail(value != NULL, FALSE);
    int64_t v = 0;
    size_t size = sizeof(v);
    const gboolean ret = _get_option(self, option, &v, &size, error);
    *value = (gint64)v;
    return ret;
}

gboolean g_easyzmq_set_option_bytes(GZmqSource *self, int option, const guint8 *value, gsize size, GError **error) {
    return _set_option(self, option, (value != NULL) ? (const void *)value : (const void *)"", (value != NULL) ? size : 0, error);
}

gboolean g_easyzmq_set_option_string(GZmqSource *self, int option, const gchar *value, GError **error) {
    return _set_option(self, option, (value != NULL) ? value : "", (value != NULL) ? strlen(value) : 0, error);
}

gchar *g_easyzmq_get_option_string(GZmqSource *self, int option, GError **error) {
    gchar buffer[256];
    size_t size = sizeof(buffer);
    if (!_get_option(self, option, buffer, &size, error)) {
        return NULL;
    }
    // zmq string options include the terminating NUL
    return g_strndup(buffer, size);
}

static void _free_heap_msg(gpointer data) {
    zmq_msg_t *const msg = (zmq_msg_t *)data;
    assert_errno(zmq_msg_close(msg) == 0, "error %s[%d] on zmq_msg_close", clean_zmq_errno(), zmq_errno());
    g_slice_free(zmq_msg_t, msg);
}

GBytes *g_easyzmq_get_current_bytes(GZmqSource *self) {
    g_return_val_if_fail(self != NULL, NULL);
    if (ISNOTNULL(self->current_msg)) {
        // zero copy (the zmq message data is reference counted, small messages are copied)
        zmq_msg_t *const msg = g_slice_new(zmq_msg_t);
        assert_errno(zmq_msg_init(msg) == 0, "error %s[%d] on zmq_msg_init", clean_zmq_errno(), zmq_errno());
        assert_errno(zmq_msg_copy(msg, self->current_msg) == 0, "error %s[%d] on zmq_msg_copy", clean_zmq_errno(), zmq_errno());
        return g_bytes_new_with_free_func(zmq_msg_data(msg), zmq_msg_size(msg), _free_heap_msg, msg);
    } else if (ISNOTNULL(self->current_buf)) {
        // pre-allocated buffer is reused for the next message
        return g_bytes_new(self->current_buf, (gsize)self->current_len);
    } else {
        g_warning("g_easyzmq_get_current_bytes called outside a receive callback");
        return NULL;
    }
}

void g_easyzmq_set_paused(GZmqSource *self, gboolean paused) {
    g_return_if_fail(self != NULL);
    if (self->paused != paused) {
        self->paused = paused;
        if (!paused) {
            // messages can be already queued: the main loop has to check them
            g_main_context_wakeup(g_source_get_context((GSource *)self));
        }
    }
}

gboolean g_easyzmq_get_paused(GZmqSource *self) {
    g_return_val_if_fail(self != NULL, FALSE);
    return self->paused;
}

//

static void my_zmq_free_fn(void *data, void *hint) {
    if ((data != NULL) && (hint != NULL)) {
        GDestroyNotify destroy_data = (GDestroyNotify)hint;
        destroy_data(data);
    }
}

static void _bytes_free_fn(void *data, void *hint) {
    (void)data;
    g_bytes_unref((GBytes *)hint);
}

static void _msg_init_copy(zmq_msg_t *message, const guint8 *buf, int len) {
    g_assert(buf != NULL);
    g_assert(len > 0);
    assert_errno(zmq_msg_init_size(message, (size_t)len) == 0, "error %s[%d] on zmq_msg_init_size", clean_zmq_errno(), zmq_errno());
    memcpy(zmq_msg_data(message), buf, (size_t)len);
}

static void _msg_init_take(zmq_msg_t *message, guint8 *buf, int len, GDestroyNotify destroy_data) {
    g_assert(buf != NULL);
    g_assert(len > 0);
    assert_errno(zmq_msg_init_data(message, buf, (size_t)len, my_zmq_free_fn, (void *)destroy_data) == 0, "error %s[%d] on zmq_msg_init_data", clean_zmq_errno(), zmq_errno());
}

// zero copy: the message keeps a reference of bytes
static void _msg_init_bytes(zmq_msg_t *message, GBytes *bytes) {
    g_assert(bytes != NULL);
    gsize size = 0;
    gconstpointer const data = g_bytes_get_data(bytes, &size);
    if (size == 0) {
        assert_errno(zmq_msg_init_size(message, 0) == 0, "error %s[%d] on zmq_msg_init_size", clean_zmq_errno(), zmq_errno());
    } else {
        assert_errno(zmq_msg_init_data(message, (void *)data, size, _bytes_free_fn, g_bytes_ref(bytes)) == 0, "error %s[%d] on zmq_msg_init_data", clean_zmq_errno(), zmq_errno());
    }
}

// send a message (always consumed, also in case of error)
static gboolean _send_msg(GZmqSource *zmq_source, zmq_msg_t *message, int flags) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);

    if (zmq_source->type == EASY_ZMQ_REP) {
        _pollfdOn(zmq_source); // after the answer the next request can be received
    }

    if (G_UNLIKELY(_zmq_msg_send(message, zmq_source->zmq_owned_socket, zmq_source->send_flags | flags) == -1)) {
        g_debug("send message error (%d,%s)", zmq_errno(), clean_zmq_errno());
        assert_errno(zmq_msg_close(message) == 0, "error %s[%d] on zmq_msg_close", clean_zmq_errno(), zmq_errno());
        return FALSE;
    }
    return TRUE;
}

gboolean g_easyzmq_send_bytes(GZmqSource *zmq_source, GBytes *bytes) {
    g_return_val_if_fail(zmq_source != NULL, FALSE);
    g_return_val_if_fail(bytes != NULL, FALSE);
    g_return_val_if_fail(_easyzmq_type_can_send(zmq_source->type), FALSE);

    zmq_msg_t message;
    _msg_init_bytes(&message, bytes);
    return _send_msg(zmq_source, &message, 0);
}

gboolean g_easyzmq_send(GZmqSource *zmq_source, const guint8 *buf, int len) {
    g_return_val_if_fail(zmq_source != NULL, FALSE);
    g_return_val_if_fail(_easyzmq_type_can_send(zmq_source->type), FALSE);

    zmq_msg_t message;
    _msg_init_copy(&message, buf, len);
    return _send_msg(zmq_source, &message, 0);
}

gboolean g_easyzmq_send_take(GZmqSource *zmq_source, guint8 *buf, int len, GDestroyNotify destroy_data) {
    g_return_val_if_fail(zmq_source != NULL, FALSE);
    g_return_val_if_fail(_easyzmq_type_can_send(zmq_source->type), FALSE);

    zmq_msg_t message;
    _msg_init_take(&message, buf, len, destroy_data);
    return _send_msg(zmq_source, &message, 0);
}

gboolean g_easyzmq_send_more(GZmqSource *zmq_source, const guint8 *buf, int len) {
    g_return_val_if_fail(zmq_source != NULL, FALSE);
    g_return_val_if_fail(_easyzmq_type_can_send(zmq_source->type), FALSE);
    g_return_val_if_fail(len >= 0, FALSE);

    zmq_msg_t message;
    if (len == 0) {
        assert_errno(zmq_msg_init_size(&message, 0) == 0, "error %s[%d] on zmq_msg_init_size", clean_zmq_errno(), zmq_errno());
    } else {
        _msg_init_copy(&message, buf, len);
    }
    return _send_msg(zmq_source, &message, ZMQ_SNDMORE);
}

gboolean g_easyzmq_send_bytes_more(GZmqSource *zmq_source, GBytes *bytes) {
    g_return_val_if_fail(zmq_source != NULL, FALSE);
    g_return_val_if_fail(bytes != NULL, FALSE);
    g_return_val_if_fail(_easyzmq_type_can_send(zmq_source->type), FALSE);

    zmq_msg_t message;
    _msg_init_bytes(&message, bytes);
    return _send_msg(zmq_source, &message, ZMQ_SNDMORE);
}

gboolean g_easyzmq_recv(GZmqSource *zmq_source, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify) {
    g_return_val_if_fail(zmq_source != NULL, FALSE);
    g_return_val_if_fail(_easyzmq_type_can_recv(zmq_source->type), FALSE);

    if (zmq_source->type == EASY_ZMQ_REP) {
        return recv_zmq_rep(zmq_source, cb, user_data, notify);
    }
    if (ISNULL(cb)) {
        cb = fake_callback;
    }
    g_source_set_callback((GSource *)zmq_source, (GSourceFunc)(void *)cb, user_data, notify);
    return TRUE;
}

// receive with GBytes

typedef struct bytes_callback_data {
    GZmqSourceBytesFunc callback;
    gpointer user_data;
    GDestroyNotify notify;
} bytes_callback_data_t;

static void bytes_callback_data_free(gpointer data) {
    bytes_callback_data_t *const pdata = (bytes_callback_data_t *)data;
    if (ISNOTNULL(pdata->notify) && ISNOTNULL(pdata->user_data)) {
        pdata->notify(pdata->user_data);
    }
    g_slice_free(bytes_callback_data_t, pdata);
}

static gboolean bytes_callback(GZmqSource *zmq_source, const guint8 *buf, int len, gboolean more, gpointer user_data) {
    (void)len;
    bytes_callback_data_t *const pdata = (bytes_callback_data_t *)user_data;
    GBytes *const bytes = ISNOTNULL(buf) ? g_easyzmq_get_current_bytes(zmq_source) : NULL;
    g_zmq_source_ref(zmq_source);
    if (ISNOTNULL(pdata->callback)) {
        pdata->callback(zmq_source, bytes, more, pdata->user_data);
    }
    g_zmq_source_unref(zmq_source);
    if (ISNOTNULL(bytes)) {
        g_bytes_unref(bytes);
    }
    return G_SOURCE_CONTINUE;
}

gboolean g_easyzmq_recv_bytes(GZmqSource *zmq_source, GZmqSourceBytesFunc cb, gpointer user_data, GDestroyNotify notify) {
    g_return_val_if_fail(zmq_source != NULL, FALSE);
    g_return_val_if_fail(_easyzmq_type_can_recv(zmq_source->type), FALSE);

    bytes_callback_data_t *const pdata = g_slice_new0(bytes_callback_data_t);
    pdata->callback = cb;
    pdata->user_data = user_data;
    pdata->notify = notify;
    g_source_set_callback((GSource *)zmq_source, (GSourceFunc)(void *)bytes_callback, pdata, bytes_callback_data_free);
    return TRUE;
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

    g_source_set_callback((GSource *)zmq_source, (GSourceFunc)(void *)rep_callback, pdata, rep_callback_data_free);

    return TRUE;
}

gboolean send_zmq_rep(GZmqSource *zmq_source, const guint8 *buf, int len) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->type == EASY_ZMQ_REP);

    zmq_msg_t message;
    _msg_init_copy(&message, buf, len);
    return _send_msg(zmq_source, &message, 0);
}

gboolean send_zmq_rep_take(GZmqSource *zmq_source, guint8 *buf, int len, GDestroyNotify destroy_data) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->type == EASY_ZMQ_REP);

    zmq_msg_t message;
    _msg_init_take(&message, buf, len, destroy_data);
    return _send_msg(zmq_source, &message, 0);
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

    g_source_set_callback((GSource *)zmq_source, (GSourceFunc)(void *)router_callback, pdata, router_callback_data_free);

    return TRUE;
}

void destroy_identity(void *pdata) {
    g_assert(pdata != NULL);
    GList *identity = (GList *)pdata;
    g_assert(identity != NULL);

    while (ISNOTNULL(identity)) {
        GList *const identity_element = identity;
        g_assert(identity_element->data != NULL);
        identity = g_list_remove_link(identity, identity_element);
        idenity_list_element_t *const elem = (idenity_list_element_t *)(identity_element->data);
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
    zmq_msg_t message;
    _msg_init_copy(&message, buf, len);
    return _send_router_msg(zmq_source, pdata, &message);
}

gboolean send_zmq_router_take(GZmqSource *zmq_source, void *pdata, guint8 *buf, int len, GDestroyNotify destroy_data) {
    zmq_msg_t message;
    _msg_init_take(&message, buf, len, destroy_data);
    return _send_router_msg(zmq_source, pdata, &message);
}

gboolean send_zmq_router_bytes(GZmqSource *zmq_source, void *identity, GBytes *bytes) {
    g_return_val_if_fail(bytes != NULL, FALSE);
    zmq_msg_t message;
    _msg_init_bytes(&message, bytes);
    return _send_router_msg(zmq_source, identity, &message);
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
    zmq_msg_t message;
    _msg_init_copy(&message, buf, len);
    return _send_zmq_req_msg(zmq_source, timeoutms, &message, cb, user_data, notify);
}

gboolean send_zmq_req_take(GZmqSource *zmq_source, guint timeoutms, guint8 *buf, int len, GDestroyNotify destroy_data, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify) {
    zmq_msg_t message;
    _msg_init_take(&message, buf, len, destroy_data);
    return _send_zmq_req_msg(zmq_source, timeoutms, &message, cb, user_data, notify);
}

gboolean send_zmq_req_bytes(GZmqSource *zmq_source, guint timeoutms, GBytes *bytes, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify) {
    g_return_val_if_fail(bytes != NULL, FALSE);
    zmq_msg_t message;
    _msg_init_bytes(&message, bytes);
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

    g_source_set_callback((GSource *)zmq_source, (GSourceFunc)(void *)cb, user_data, notify);

    return TRUE;
}

// PUB SOCKET

gboolean send_zmq_pub(GZmqSource *zmq_source, const guint8 *buf, int len) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->type == EASY_ZMQ_PUB);

    zmq_msg_t message;
    _msg_init_copy(&message, buf, len);
    return _send_msg(zmq_source, &message, 0);
}

gboolean send_zmq_pub_take(GZmqSource *zmq_source, guint8 *buf, int len, GDestroyNotify destroy_data) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->type == EASY_ZMQ_PUB);

    zmq_msg_t message;
    _msg_init_take(&message, buf, len, destroy_data);
    return _send_msg(zmq_source, &message, 0);
}

// PAIR SOCKET

gboolean recv_zmq_pair(GZmqSource *zmq_source, GZmqSourceFunc cb, gpointer user_data, GDestroyNotify notify) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->type == EASY_ZMQ_PAIR);
    if (cb == NULL) {
        cb = fake_callback;
    }
    g_source_set_callback((GSource *)zmq_source, (GSourceFunc)(void *)cb, user_data, notify);

    return TRUE;
}

gboolean send_zmq_pair(GZmqSource *zmq_source, const guint8 *buf, int len) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->type == EASY_ZMQ_PAIR);

    zmq_msg_t message;
    _msg_init_copy(&message, buf, len);
    return _send_msg(zmq_source, &message, 0);
}

gboolean send_zmq_pair_take(GZmqSource *zmq_source, guint8 *buf, int len, GDestroyNotify destroy_data) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->type == EASY_ZMQ_PAIR);

    zmq_msg_t message;
    _msg_init_take(&message, buf, len, destroy_data);
    return _send_msg(zmq_source, &message, 0);
}

// Monitor

typedef struct monitor_callback_data {
    int state;
    guint32 value;
    easyzmq_monitor_event_t event;
    GZmqMonitorFunc callback;          // old API: some events discarded, no user_data
    GZmqMonitorFullFunc full_callback; // all events, with user_data
    gpointer user_data;
    GDestroyNotify notify;
} monitor_callback_data_t;

static void monitor_callback_data_free(gpointer data) {
    monitor_callback_data_t *const pdata = (monitor_callback_data_t *)data;
    if (ISNOTNULL(pdata->notify) && ISNOTNULL(pdata->user_data)) {
        pdata->notify(pdata->user_data);
    }
    g_slice_free(monitor_callback_data_t, pdata);
}

static easyzmq_monitor_event_t _monitor_event_from_zmq(guint16 zmq_event) {
    switch (zmq_event) {
    case ZMQ_EVENT_CONNECTED:
        return EASYZMQ_EVENT_CONNECTED;
    case ZMQ_EVENT_CONNECT_DELAYED:
        return EASYZMQ_EVENT_CONNECT_DELAYED;
    case ZMQ_EVENT_CONNECT_RETRIED:
        return EASYZMQ_EVENT_CONNECT_RETRIED;
    case ZMQ_EVENT_LISTENING:
        return EASYZMQ_EVENT_LISTENING;
    case ZMQ_EVENT_BIND_FAILED:
        return EASYZMQ_EVENT_BIND_FAILED;
    case ZMQ_EVENT_ACCEPTED:
        return EASYZMQ_EVENT_ACCEPTED;
    case ZMQ_EVENT_ACCEPT_FAILED:
        return EASYZMQ_EVENT_ACCEPT_FAILED;
    case ZMQ_EVENT_CLOSED:
        return EASYZMQ_EVENT_CLOSED;
    case ZMQ_EVENT_CLOSE_FAILED:
        return EASYZMQ_EVENT_CLOSE_FAILED;
    case ZMQ_EVENT_DISCONNECTED:
        return EASYZMQ_EVENT_DISCONNECTED;
    case ZMQ_EVENT_MONITOR_STOPPED:
        return EASYZMQ_EVENT_MONITOR_STOPPED;
#ifdef ZMQ_EVENT_HANDSHAKE_FAILED_NO_DETAIL // libzmq >= 4.3
    case ZMQ_EVENT_HANDSHAKE_FAILED_NO_DETAIL:
        return EASYZMQ_EVENT_HANDSHAKE_FAILED_NO_DETAIL;
    case ZMQ_EVENT_HANDSHAKE_SUCCEEDED:
        return EASYZMQ_EVENT_HANDSHAKE_SUCCEEDED;
    case ZMQ_EVENT_HANDSHAKE_FAILED_PROTOCOL:
        return EASYZMQ_EVENT_HANDSHAKE_FAILED_PROTOCOL;
    case ZMQ_EVENT_HANDSHAKE_FAILED_AUTH:
        return EASYZMQ_EVENT_HANDSHAKE_FAILED_AUTH;
#endif
    default:
        return EASYZMQ_EVENT_UNKNOWN;
    }
}

static gboolean get_easyzmq_monitor_event(const guint8 *buf, int len, easyzmq_monitor_event_t *event, guint32 *value) {
    // The first frame contains an event number (16 bits), and an event value (32 bits) that provides additional data according to the event number.
    if (6 == len) {
        g_assert(buf != NULL);
        guint16 zmq_event = 0;
        guint32 zmq_value = 0;
        memcpy(&zmq_event, buf, sizeof(zmq_event));
        memcpy(&zmq_value, buf + sizeof(zmq_event), sizeof(zmq_value));
        if (value) {
            *value = zmq_value;
        }
        if (event) {
            *event = _monitor_event_from_zmq(zmq_event);
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
        if ((pdata->state == 0) && get_easyzmq_monitor_event(buf, len, &(pdata->event), &(pdata->value))) {
            pdata->state = 1;
        } else {
            pdata->state = 2; // discard the message
        }
    } else {
        const int state = pdata->state;
        pdata->state = 0;
        if (state != 1) {
            return TRUE;
        }
        if (ISNOTNULL(pdata->full_callback)) {
            gchar *const address = g_strndup((const gchar *)buf, (gsize)len);
            pdata->full_callback(zmq_source, pdata->event, pdata->value, address, pdata->user_data);
            g_free(address);
        } else if ((pdata->callback) && (pdata->event != EASYZMQ_EVENT_CONNECT_DELAYED) && (pdata->event != EASYZMQ_EVENT_CONNECT_RETRIED) && (pdata->event != EASYZMQ_EVENT_CLOSED) && (pdata->event <= EASYZMQ_EVENT_MONITOR_STOPPED)) {
            pdata->callback(zmq_source, pdata->event, pdata->value, buf, len);
        }
    }

    return TRUE;
}

#define ZMQ_EVENT_ATTACHED (ZMQ_EVENT_ALL)

// create the monitor (if needed) and register the callback data, return a new reference
static GZmqSource *_get_zmq_socket_monitor(GZmqSource *zmq_source, monitor_callback_data_t *pdata) {
    g_assert(zmq_source != NULL);
    g_assert(zmq_source->zmq_owned_socket != NULL);
    g_assert(pdata != NULL);

    if (ISNULL(zmq_source->zmq_source_monitor)) {
        guint unique = get_unique();
        gchar address[48];
        g_snprintf(address, G_N_ELEMENTS(address), "inproc://easyzmq-monitor-%u", unique);

        if (zmq_socket_monitor(zmq_source->zmq_owned_socket, address, ZMQ_EVENT_ATTACHED) != 0) {
            g_warning("error creating monitor socket: %s", clean_zmq_errno());
            monitor_callback_data_free(pdata);
            return NULL;
        }
        GZmqSource *const monitor = g_zmq_source_pair_new(0, g_source_get_context((GSource *)zmq_source));
        if (ISNULL(monitor)) {
            g_warning("error creating monitor pair socket");
            zmq_socket_monitor(zmq_source->zmq_owned_socket, NULL, 0);
            monitor_callback_data_free(pdata);
            return NULL;
        }
        recv_zmq_pair(monitor, monitor_callback, pdata, monitor_callback_data_free);
        if (!g_easyzmq_connect(monitor, address, NULL)) {
            zmq_socket_monitor(zmq_source->zmq_owned_socket, NULL, 0);
            g_zmq_source_unref(monitor);
            return NULL;
        }
        zmq_source->zmq_source_monitor = monitor; // reference owned by the monitored source
    } else {
        // replace the callback of the existing monitor
        recv_zmq_pair(zmq_source->zmq_source_monitor, monitor_callback, pdata, monitor_callback_data_free);
    }

    return g_zmq_source_ref(zmq_source->zmq_source_monitor);
}

GZmqSource *get_zmq_socket_monitor(GZmqSource *zmq_source, GZmqMonitorFunc cb) {
    g_assert(zmq_source != NULL);
    if (ISNOTNULL(zmq_source->zmq_source_monitor) && ISNULL(cb)) {
        // only a new reference of the existing monitor
        return g_zmq_source_ref(zmq_source->zmq_source_monitor);
    }
    monitor_callback_data_t *const pdata = g_slice_new0(monitor_callback_data_t);
    pdata->callback = cb;
    return _get_zmq_socket_monitor(zmq_source, pdata);
}

GZmqSource *get_zmq_socket_monitor_full(GZmqSource *zmq_source, GZmqMonitorFullFunc cb, gpointer user_data, GDestroyNotify notify) {
    g_assert(zmq_source != NULL);
    monitor_callback_data_t *const pdata = g_slice_new0(monitor_callback_data_t);
    pdata->full_callback = cb;
    pdata->user_data = user_data;
    pdata->notify = notify;
    return _get_zmq_socket_monitor(zmq_source, pdata);
}
