// automatic regression test (inproc only, no network): run with "meson test"

#include "easyzmq.h"

#include <gio/gio.h>
#include <glib.h>
#include <string.h>
#include <zmq.h>

static GMainLoop *loop = NULL;

static gboolean quit_cb(G_GNUC_UNUSED gpointer user_data) {
    g_main_loop_quit(loop);
    return G_SOURCE_REMOVE;
}

static void run_loop_ms(guint ms) {
    g_timeout_add(ms, quit_cb, NULL);
    g_main_loop_run(loop);
}

// ---- REP echo server, user_data must be valid for every message

static int rep_user_data_value = 42;
static int rep_received = 0;

static gboolean rep_echo_cb(GZmqSource *zmq_source, const guint8 *buf, int len, G_GNUC_UNUSED gboolean more, gpointer user_data) {
    g_assert(buf != NULL);
    g_assert(user_data == &rep_user_data_value);
    rep_received++;
    g_assert(send_zmq_rep(zmq_source, buf, len));
    return G_SOURCE_CONTINUE;
}

static GZmqSource *new_rep_server(const char *addr) {
    GZmqSource *rep = g_zmq_source_rep_new(0, NULL);
    g_assert(rep != NULL);
    g_assert(g_easyzmq_bind(rep, addr, NULL));
    g_assert(recv_zmq_rep(rep, rep_echo_cb, &rep_user_data_value, NULL));
    return rep;
}

// ---- chained requests: a new request is sent from inside the answer callback

static int chain_answers = 0;
static int chain_notify = 0;
#define CHAIN_COUNT 50

static void chain_notify_cb(gpointer data) {
    g_assert(data != NULL);
    chain_notify++;
}

static gboolean chain_cb(GZmqSource *zmq_source, const guint8 *buf, int len, gboolean more, gpointer user_data) {
    g_assert(buf != NULL);
    g_assert(user_data == GINT_TO_POINTER(chain_answers + 1));
    g_assert(!more);
    g_assert(len == sizeof(int));
    int value;
    memcpy(&value, buf, sizeof(value));
    g_assert(value == chain_answers);
    chain_answers++;
    if (chain_answers < CHAIN_COUNT) {
        g_assert(send_zmq_req(zmq_source, 1000, (const guint8 *)&chain_answers, sizeof(chain_answers), chain_cb, GINT_TO_POINTER(chain_answers + 1), chain_notify_cb));
    } else {
        g_main_loop_quit(loop);
    }
    return G_SOURCE_CONTINUE;
}

static void test_chain(void) {
    GZmqSource *rep = new_rep_server("inproc://chain");
    GZmqSource *req = g_zmq_source_req_new(0, NULL);
    g_assert(g_easyzmq_connect(req, "inproc://chain", NULL));

    int first = 0;
    g_assert(send_zmq_req(req, 1000, (const guint8 *)&first, sizeof(first), chain_cb, GINT_TO_POINTER(1), chain_notify_cb));
    run_loop_ms(3000);
    g_assert_cmpint(chain_answers, ==, CHAIN_COUNT);
    g_assert_cmpint(chain_notify, ==, CHAIN_COUNT);
    g_assert_cmpint(rep_received, >=, CHAIN_COUNT);

    g_zmq_source_close(&req);
    g_zmq_source_close(&rep);
}

// ---- take variant has to receive the answer (fd must be polled)

static int take_answers = 0;

static gboolean take_cb(G_GNUC_UNUSED GZmqSource *zmq_source, const guint8 *buf, int len, G_GNUC_UNUSED gboolean more, G_GNUC_UNUSED gpointer user_data) {
    g_assert(buf != NULL);
    g_assert_cmpint(len, ==, 6);
    g_assert(memcmp(buf, "hello", 6) == 0);
    take_answers++;
    g_main_loop_quit(loop);
    return G_SOURCE_CONTINUE;
}

static void test_take(void) {
    GZmqSource *rep = new_rep_server("inproc://take");
    GZmqSource *req = g_zmq_source_req_new(64, NULL);
    g_assert(g_easyzmq_connect(req, "inproc://take", NULL));

    g_assert(send_zmq_req_take_easy(req, 0, (guint8 *)g_strdup("hello"), 6, g_free, take_cb, NULL));
    run_loop_ms(2000);
    g_assert_cmpint(take_answers, ==, 1);

    g_zmq_source_close(&req);
    g_zmq_source_close(&rep);
}

// ---- timeout: callback called with NULL, and destroying the source with a pending timeout is safe

static int timeout_calls = 0;
static int timeout_notify = 0;

static gboolean timeout_cb(G_GNUC_UNUSED GZmqSource *zmq_source, const guint8 *buf, G_GNUC_UNUSED int len, G_GNUC_UNUSED gboolean more, G_GNUC_UNUSED gpointer user_data) {
    g_assert(buf == NULL);
    timeout_calls++;
    return G_SOURCE_CONTINUE;
}

static void timeout_notify_cb(G_GNUC_UNUSED gpointer data) {
    timeout_notify++;
}

static gboolean silent_rep_cb(G_GNUC_UNUSED GZmqSource *zmq_source, G_GNUC_UNUSED const guint8 *buf, G_GNUC_UNUSED int len, G_GNUC_UNUSED gboolean more, G_GNUC_UNUSED gpointer user_data) {
    return G_SOURCE_CONTINUE; // never answer
}

static void test_timeout(void) {
    GZmqSource *rep = g_zmq_source_rep_new(0, NULL);
    g_assert(g_easyzmq_bind(rep, "inproc://timeout", NULL));
    g_assert(recv_zmq_rep(rep, silent_rep_cb, NULL, NULL));

    GZmqSource *req = g_zmq_source_req_new(0, NULL);
    g_assert(g_easyzmq_connect(req, "inproc://timeout", NULL));

    g_assert(send_zmq_req(req, 100, (const guint8 *)"x", 1, timeout_cb, GINT_TO_POINTER(1), timeout_notify_cb));
    run_loop_ms(300);
    g_assert_cmpint(timeout_calls, ==, 1);
    g_assert_cmpint(timeout_notify, ==, 1);

    // relaxed mode: a new request is accepted after a timeout
    g_assert(send_zmq_req(req, 100, (const guint8 *)"y", 1, timeout_cb, GINT_TO_POINTER(1), timeout_notify_cb));
    // destroy the source with the timeout still pending
    g_zmq_source_close(&req);
    g_assert_cmpint(timeout_notify, ==, 2);
    run_loop_ms(300); // the timeout must not fire on freed memory
    g_assert_cmpint(timeout_calls, ==, 1);

    g_zmq_source_close(&rep);
}

// ---- send failure must not break the pending request (strict REQ: EFSM)

static int strict_answers = 0;

static gboolean strict_cb(G_GNUC_UNUSED GZmqSource *zmq_source, const guint8 *buf, G_GNUC_UNUSED int len, G_GNUC_UNUSED gboolean more, gpointer user_data) {
    g_assert(buf != NULL);
    g_assert(user_data == GINT_TO_POINTER(1));
    strict_answers++;
    g_main_loop_quit(loop);
    return G_SOURCE_CONTINUE;
}

static void test_strict_resend(void) {
    GZmqSource *rep = new_rep_server("inproc://strict");
    GZmqSource *req = g_zmq_source_req_new(0, NULL);
    g_assert(g_easyzmq_connect(req, "inproc://strict", NULL));

    g_assert(send_zmq_req(req, 0, (const guint8 *)"a", 1, strict_cb, GINT_TO_POINTER(1), NULL));
    // second request while the first is pending: refused, first one still alive
    g_assert(!send_zmq_req(req, 0, (const guint8 *)"b", 1, strict_cb, GINT_TO_POINTER(2), NULL));
    run_loop_ms(2000);
    g_assert_cmpint(strict_answers, ==, 1);

    g_zmq_source_close(&req);
    g_zmq_source_close(&rep);
}

// ---- router: answer reaches the right REQ, user_data valid for every message

static int router_user_data_value = 7;
static int router_received = 0;

static gboolean router_cb(GZmqSource *zmq_source, void *identity, const guint8 *buf, int len, gboolean more, gpointer user_data) {
    g_assert(user_data == &router_user_data_value);
    g_assert(identity != NULL);
    g_assert(!more);
    router_received++;
    g_assert(send_zmq_router(zmq_source, identity, buf, len));
    return G_SOURCE_CONTINUE;
}

static int router_answers = 0;

static gboolean router_req_cb(G_GNUC_UNUSED GZmqSource *zmq_source, const guint8 *buf, int len, G_GNUC_UNUSED gboolean more, gpointer user_data) {
    g_assert(buf != NULL);
    g_assert_cmpint(len, ==, 1);
    g_assert(buf[0] == (guint8)GPOINTER_TO_INT(user_data));
    router_answers++;
    if (router_answers == 2) {
        g_main_loop_quit(loop);
    }
    return G_SOURCE_CONTINUE;
}

static void test_router(void) {
    GZmqSource *router = g_zmq_source_router_new(0, NULL);
    g_assert(g_easyzmq_bind(router, "inproc://router", NULL));
    g_assert(recv_zmq_router(router, router_cb, &router_user_data_value, NULL));

    GZmqSource *req1 = g_zmq_source_req_new(0, NULL);
    GZmqSource *req2 = g_zmq_source_req_new(0, NULL);
    g_assert(g_easyzmq_connect(req1, "inproc://router", NULL));
    g_assert(g_easyzmq_connect(req2, "inproc://router", NULL));

    g_assert(send_zmq_req(req1, 1000, (const guint8 *)"1", 1, router_req_cb, GINT_TO_POINTER('1'), NULL));
    g_assert(send_zmq_req(req2, 1000, (const guint8 *)"2", 1, router_req_cb, GINT_TO_POINTER('2'), NULL));
    run_loop_ms(2000);
    g_assert_cmpint(router_received, ==, 2);
    g_assert_cmpint(router_answers, ==, 2);

    g_zmq_source_close(&req1);
    g_zmq_source_close(&req2);
    g_zmq_source_close(&router);
}

// ---- closing the source from inside its callback

static int close_calls = 0;

static gboolean close_in_cb(GZmqSource *zmq_source, G_GNUC_UNUSED const guint8 *buf, G_GNUC_UNUSED int len, G_GNUC_UNUSED gboolean more, gpointer user_data) {
    close_calls++;
    GZmqSource **pself = (GZmqSource **)user_data;
    g_assert(*pself == zmq_source);
    g_zmq_source_close(pself);
    return G_SOURCE_CONTINUE;
}

static void test_close_in_callback(void) {
    static GZmqSource *sub = NULL;
    GZmqSource *pub = g_zmq_source_pub_new(NULL);
    g_assert(g_easyzmq_bind(pub, "inproc://pubsub", NULL));
    sub = g_zmq_source_sub_new(0, NULL);
    g_assert(g_easyzmq_connect(sub, "inproc://pubsub", NULL));
    g_assert(subscribe_zmq_sub(sub, NULL, 0, NULL));
    g_assert(recv_zmq_sub(sub, close_in_cb, &sub, NULL));
    run_loop_ms(50); // let the subscription arrive

    for (int i = 0; i < 10; i++) {
        g_assert(send_zmq_pub(pub, (const guint8 *)"m", 1));
    }
    run_loop_ms(200);
    g_assert_cmpint(close_calls, ==, 1);
    g_assert(sub == NULL);

    g_zmq_source_close(&pub);
}

// ---- not blocking send (default) and blocking send

static void test_dontwait(void) {
    GZmqSource *pair = g_zmq_source_pair_new(0, NULL);
    g_assert(g_easyzmq_bind(pair, "inproc://dontwait", NULL));
    g_assert(!g_easyzmq_get_blocking_send(pair));
    // no peer connected: with the old blocking send this call never returned
    const gint64 start = g_get_monotonic_time();
    g_assert(!send_zmq_pair(pair, (const guint8 *)"x", 1));
    g_assert_cmpint(g_get_monotonic_time() - start, <, G_USEC_PER_SEC);
    g_zmq_source_close(&pair);
}

// ---- source created from a user socket

static int from_socket_received = 0;

static gboolean from_socket_cb(G_GNUC_UNUSED GZmqSource *zmq_source, const guint8 *buf, int len, G_GNUC_UNUSED gboolean more, G_GNUC_UNUSED gpointer user_data) {
    g_assert(buf != NULL);
    g_assert_cmpint(len, ==, 3);
    from_socket_received++;
    g_main_loop_quit(loop);
    return G_SOURCE_CONTINUE;
}

static void test_from_socket(void) {
    void *raw = zmq_socket(get_zmq_context(), ZMQ_PAIR);
    g_assert(raw != NULL);
    int hwm = 10;
    g_assert(zmq_setsockopt(raw, ZMQ_RCVHWM, &hwm, sizeof(hwm)) == 0);
    GZmqSource *a = g_zmq_source_new_from_socket(raw, 0, NULL);
    g_assert(a != NULL);
    g_assert(g_easyzmq_get_socket_type(a) == EASY_ZMQ_PAIR);
    g_assert(g_easyzmq_bind(a, "inproc://fromsocket", NULL));
    g_assert(recv_zmq_pair(a, from_socket_cb, NULL, NULL));

    GZmqSource *b = g_zmq_source_pair_new(0, NULL);
    g_assert(g_easyzmq_connect(b, "inproc://fromsocket", NULL));
    g_assert(send_zmq_pair(b, (const guint8 *)"abc", 3));
    run_loop_ms(2000);
    g_assert_cmpint(from_socket_received, ==, 1);

    // not supported socket type: closed and NULL returned
    void *raw2 = zmq_socket(get_zmq_context(), ZMQ_STREAM);
    g_assert(raw2 != NULL);
    g_assert(g_zmq_source_new_from_socket(raw2, 0, NULL) == NULL);

    g_zmq_source_close(&b);
    g_zmq_source_close(&a);
}

// ---- init is reference counted

static void test_init_refcount(void) {
    void *const ctx = get_zmq_context();
    easy_zmq_init(); // second user (main already initialized)
    g_assert(get_zmq_context() == ctx);
    easy_zmq_init_with_context(ctx);
    easy_zmq_deinit();
    easy_zmq_deinit();
    g_assert(get_zmq_context() == ctx); // still valid: main is still a user
}

// ---- GTask async API

typedef struct {
    GBytes *bytes;
    GError *error;
    gboolean finished;
} async_result_t;

static void request_done(GObject *object, GAsyncResult *res, gpointer user_data) {
    (void)object;
    async_result_t *r = user_data;
    r->bytes = g_easyzmq_request_finish(NULL, res, &r->error);
    r->finished = TRUE;
    g_main_loop_quit(loop);
}

static void receive_done(GObject *object, GAsyncResult *res, gpointer user_data) {
    (void)object;
    async_result_t *r = user_data;
    r->bytes = g_easyzmq_receive_finish(NULL, res, &r->error);
    r->finished = TRUE;
}

static void async_result_clear(async_result_t *r) {
    if (r->bytes) {
        g_bytes_unref(r->bytes);
    }
    g_clear_error(&r->error);
    r->bytes = NULL;
    r->finished = FALSE;
}

static void test_task_request(void) {
    GZmqSource *rep = new_rep_server("inproc://task-req");
    GZmqSource *req = g_zmq_source_req_new(0, NULL);
    g_assert(g_easyzmq_connect(req, "inproc://task-req", NULL));

    async_result_t r = {0};
    GBytes *msg = g_bytes_new_static("ping", 4);
    g_easyzmq_request_async(req, msg, 1000, NULL, request_done, &r);
    run_loop_ms(2000);
    g_assert(r.finished);
    g_assert_no_error(r.error);
    g_assert(g_bytes_equal(r.bytes, msg));
    async_result_clear(&r);

    // cancel: completed immediately with G_IO_ERROR_CANCELLED
    GCancellable *cancellable = g_cancellable_new();
    g_cancellable_cancel(cancellable);
    g_easyzmq_request_async(req, msg, 1000, cancellable, request_done, &r);
    run_loop_ms(2000);
    g_assert_error(r.error, G_IO_ERROR, G_IO_ERROR_CANCELLED);
    async_result_clear(&r);
    g_object_unref(cancellable);

    g_zmq_source_close(&rep);

    // timeout: nobody answers
    GZmqSource *silent = g_zmq_source_rep_new(0, NULL);
    g_assert(g_easyzmq_bind(silent, "inproc://task-silent", NULL));
    g_assert(recv_zmq_rep(silent, silent_rep_cb, NULL, NULL));
    GZmqSource *req2 = g_zmq_source_req_new(0, NULL);
    g_assert(g_easyzmq_connect(req2, "inproc://task-silent", NULL));
    g_easyzmq_request_async(req2, msg, 100, NULL, request_done, &r);
    run_loop_ms(2000);
    g_assert_error(r.error, G_IO_ERROR, G_IO_ERROR_TIMED_OUT);
    async_result_clear(&r);

    g_bytes_unref(msg);
    g_zmq_source_close(&req2);
    g_zmq_source_close(&silent);
    g_zmq_source_close(&req);
}

static void test_task_receive(void) {
    GZmqSource *a = g_zmq_source_pair_new(0, NULL);
    GZmqSource *b = g_zmq_source_pair_new(0, NULL);
    g_assert(g_easyzmq_bind(a, "inproc://task-recv", NULL));
    g_assert(g_easyzmq_connect(b, "inproc://task-recv", NULL));

    // two pending receive completed in order
    async_result_t r1 = {0}, r2 = {0};
    g_easyzmq_receive_async(a, NULL, receive_done, &r1);
    g_easyzmq_receive_async(a, NULL, receive_done, &r2);
    g_assert(send_zmq_pair(b, (const guint8 *)"one", 3));
    g_assert(send_zmq_pair(b, (const guint8 *)"two", 3));
    run_loop_ms(200);
    g_assert(r1.finished && r2.finished);
    g_assert_no_error(r1.error);
    g_assert_no_error(r2.error);
    g_assert(memcmp(g_bytes_get_data(r1.bytes, NULL), "one", 3) == 0);
    g_assert(memcmp(g_bytes_get_data(r2.bytes, NULL), "two", 3) == 0);
    async_result_clear(&r1);
    async_result_clear(&r2);

    // nobody waiting: source paused, message kept in zmq
    g_assert(g_easyzmq_get_paused(a));
    g_assert(send_zmq_pair(b, (const guint8 *)"three", 5));
    run_loop_ms(100);
    g_easyzmq_receive_async(a, NULL, receive_done, &r1);
    run_loop_ms(100);
    g_assert(r1.finished);
    g_assert_cmpuint(g_bytes_get_size(r1.bytes), ==, 5);
    async_result_clear(&r1);

    // cancel a pending receive
    GCancellable *cancellable = g_cancellable_new();
    g_easyzmq_receive_async(a, cancellable, receive_done, &r1);
    run_loop_ms(50);
    g_cancellable_cancel(cancellable);
    run_loop_ms(50);
    g_assert(r1.finished);
    g_assert_error(r1.error, G_IO_ERROR, G_IO_ERROR_CANCELLED);
    async_result_clear(&r1);
    g_object_unref(cancellable);

    // switch to the callback API: pending receive closed
    g_easyzmq_receive_async(a, NULL, receive_done, &r1);
    g_assert(recv_zmq_pair(a, NULL, NULL, NULL));
    run_loop_ms(50); // GTask callback is called from the main loop
    g_assert(r1.finished);
    g_assert_error(r1.error, G_IO_ERROR, G_IO_ERROR_CLOSED);
    g_assert(!g_easyzmq_get_paused(a));
    async_result_clear(&r1);

    g_zmq_source_close(&b);
    g_zmq_source_close(&a);
}

// ---- GBytes API

static GPtrArray *bytes_received = NULL;

static gboolean bytes_cb(G_GNUC_UNUSED GZmqSource *zmq_source, GBytes *data, G_GNUC_UNUSED gboolean more, gpointer user_data) {
    g_assert(data != NULL);
    g_assert(user_data == GINT_TO_POINTER(3));
    // the bytes are kept after the callback
    g_ptr_array_add(bytes_received, g_bytes_ref(data));
    return G_SOURCE_CONTINUE;
}

static void test_bytes(void) {
    for (int prealloc = 0; prealloc <= 1; prealloc++) {
        bytes_received = g_ptr_array_new_with_free_func((GDestroyNotify)g_bytes_unref);
        GZmqSource *a = g_zmq_source_pair_new(prealloc ? 1024 : 0, NULL);
        GZmqSource *b = g_zmq_source_pair_new(0, NULL);
        // zmq releases a closed endpoint asynchronously: one endpoint for each iteration
        const char *const endpoint = prealloc ? "inproc://bytes-prealloc" : "inproc://bytes";
        g_assert(g_easyzmq_bind(a, endpoint, NULL));
        g_assert(g_easyzmq_connect(b, endpoint, NULL));
        g_assert(g_easyzmq_recv_bytes(a, bytes_cb, GINT_TO_POINTER(3), NULL));

        // small (copied by zmq) and big (reference counted) messages
        guint8 big[1000];
        memset(big, 0xA5, sizeof(big));
        GBytes *small_msg = g_bytes_new("small", 5);
        GBytes *big_msg = g_bytes_new(big, sizeof(big));
        g_assert(g_easyzmq_send_bytes(b, small_msg));
        g_assert(g_easyzmq_send_bytes(b, big_msg));
        g_assert(g_easyzmq_send_bytes(b, small_msg));
        run_loop_ms(100);

        g_assert_cmpuint(bytes_received->len, ==, 3);
        g_assert(g_bytes_equal(g_ptr_array_index(bytes_received, 0), small_msg));
        g_assert(g_bytes_equal(g_ptr_array_index(bytes_received, 1), big_msg));
        g_assert(g_bytes_equal(g_ptr_array_index(bytes_received, 2), small_msg));

        g_bytes_unref(small_msg);
        g_bytes_unref(big_msg);
        g_zmq_source_close(&b);
        g_zmq_source_close(&a);
        g_ptr_array_unref(bytes_received); // released after the sockets
        bytes_received = NULL;
    }
}

// ---- socket options

static void test_options(void) {
    GZmqSource *sub = g_zmq_source_sub_new(0, NULL);
    GError *error = NULL;
    int value = 0;
    g_assert(g_easyzmq_set_option_int(sub, ZMQ_RCVHWM, 77, &error));
    g_assert_no_error(error);
    g_assert(g_easyzmq_get_option_int(sub, ZMQ_RCVHWM, &value, &error));
    g_assert_cmpint(value, ==, 77);
    gint64 maxsize = 0;
    g_assert(g_easyzmq_set_option_int64(sub, ZMQ_MAXMSGSIZE, 4096, &error));
    g_assert(g_easyzmq_get_option_int64(sub, ZMQ_MAXMSGSIZE, &maxsize, &error));
    g_assert_cmpint(maxsize, ==, 4096);
    g_assert(g_easyzmq_bind(sub, "inproc://options", NULL));
    gchar *endpoint = g_easyzmq_get_option_string(sub, ZMQ_LAST_ENDPOINT, &error);
    g_assert_cmpstr(endpoint, ==, "inproc://options");
    g_free(endpoint);
    // invalid option
    g_assert(!g_easyzmq_set_option_int(sub, 99999, 1, &error));
    g_assert_error(error, EASY_ZMQ_ERROR, EINVAL);
    g_clear_error(&error);
    g_assert(g_easyzmq_get_socket(sub) != NULL);
    g_zmq_source_close(&sub);
}

int main(int argc, char **argv) {
    g_test_init(&argc, &argv, NULL);
    easy_zmq_init();
    loop = g_main_loop_new(NULL, FALSE);

    g_test_add_func("/easyzmq/req-rep-chain", test_chain);
    g_test_add_func("/easyzmq/req-take", test_take);
    g_test_add_func("/easyzmq/req-timeout", test_timeout);
    g_test_add_func("/easyzmq/req-strict-resend", test_strict_resend);
    g_test_add_func("/easyzmq/router", test_router);
    g_test_add_func("/easyzmq/close-in-callback", test_close_in_callback);
    g_test_add_func("/easyzmq/dontwait", test_dontwait);
    g_test_add_func("/easyzmq/from-socket", test_from_socket);
    g_test_add_func("/easyzmq/init-refcount", test_init_refcount);
    g_test_add_func("/easyzmq/task-request", test_task_request);
    g_test_add_func("/easyzmq/task-receive", test_task_receive);
    g_test_add_func("/easyzmq/bytes", test_bytes);
    g_test_add_func("/easyzmq/options", test_options);

    const int ret = g_test_run();

    g_main_loop_unref(loop);
    easy_zmq_deinit();
    return ret;
}
