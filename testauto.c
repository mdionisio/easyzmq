// automatic regression test (inproc only, no network): run with "meson test"

#include "easyzmq.h"

#include <glib.h>
#include <string.h>

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

    const int ret = g_test_run();

    g_main_loop_unref(loop);
    easy_zmq_deinit();
    return ret;
}
