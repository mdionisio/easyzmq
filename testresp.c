// test with:
// export G_MESSAGES_DEBUG=all

/* 
 * File:   test.c
 * Author: mdionisio
 *
 * Created on 5 dicembre 2016, 12.22
 */

#include <stdio.h>
#include <stdlib.h>

#include "easyzmq.h"
#include <zmq.h>
#include <glib-unix.h>

#include <assert.h>

#include <string.h>

#include <sys/time.h>
#include <errno.h>

static void *zmq_context = NULL;
static GMainLoop *loop = NULL;


typedef struct timeout_data {
    GZmqSource *gzmq_publisher;
} typeoutdata_t;

const char default_address[] = "tcp://0.0.0.0:5555";



static int counter = 0;

gboolean timeout_cb (gpointer user_data) {
    
    GZmqSource *zmq_source = (GZmqSource *)user_data;

    printf("sleep.\n");
    gchar *datasend = g_strdup_printf("responce received: %d\n", counter);
    size_t len1 = strlen(datasend) + 1;
    gboolean ret __attribute__((unused)) = send_zmq_resp(zmq_source, datasend, len1);
    assert( ret == TRUE );
    
    g_free(datasend);
    counter++;

    return G_SOURCE_REMOVE;
}

gboolean resp_callback (GZmqSource *zmq_source, void *buf, size_t len, gpointer user_data) {
    const gchar* data = buf;
    assert(buf != NULL);
    assert(len > 0);
    struct timeval now;
    gettimeofday(&now, NULL);
    long int msec = (now.tv_sec * 1000L + now.tv_usec/1000);
    printf("%-10ld: resp socket receive: '%s' len:%u\n", msec, data, (unsigned int) len);
    
    if (counter % 2 == 0) {
        printf("sleep...\n");
        g_timeout_add(2000, timeout_cb, zmq_source);        
    } else {
        timeout_cb(zmq_source);
    }
    
    return TRUE;
}

static gboolean signal_handler (gpointer user_data)
{
    int signo = GPOINTER_TO_INT (user_data);
    switch (signo) {
        case SIGINT:
            g_main_loop_quit(loop);
            break;
        default:
            break;
    }
    return G_SOURCE_CONTINUE;
}

/*
 * 
 */
int main(int argc, char** argv) {
    const char *address = default_address;
    int rc __attribute__((unused));
    zmq_context = zmq_ctx_new ();
    
    loop = g_main_loop_new(NULL, FALSE);
    
    void *responder = zmq_socket (zmq_context, ZMQ_REP);    
    
    {
        int linger = 1000;
        zmq_setsockopt(responder, ZMQ_LINGER, &linger, sizeof(linger));
    }
    
    {
        int timeoutms = 1000;
        zmq_setsockopt(responder, ZMQ_RCVTIMEO, &timeoutms, sizeof(timeoutms));
    }
    
    {
        int timeoutms = 1000;
        zmq_setsockopt(responder, ZMQ_SNDTIMEO, &timeoutms, sizeof(timeoutms));
    }
  
    if (argc >= 2) {
        address = argv[1];
    }
    printf("address: %s\n", address);
    rc = zmq_bind (responder, address);
    assert (rc == 0);
    
    GZmqSource *gzmq_responder = g_zmq_source_new(g_main_loop_get_context(loop), zmq_context, responder, 0);
    g_debug("[%s:%d] gzmq_responder created (%p)", __func__, __LINE__, gzmq_responder);
    
    assert( recv_zmq_resp(gzmq_responder, resp_callback, NULL, NULL) == TRUE);

    g_unix_signal_add (SIGINT, signal_handler, GINT_TO_POINTER (SIGINT));
    
    g_debug("[%s:%d] start mainloop", __func__, __LINE__);
        
    g_main_loop_run(loop);

    g_debug("[%s:%d] stop mainloop", __func__, __LINE__);
    
    if (loop != NULL) {
        g_debug("[%s:%d] unref mainloop", __func__, __LINE__);
        g_main_loop_unref(loop);
        loop = NULL;
    }
    
    if (gzmq_responder != NULL) {
        g_debug("[%s:%d] destroy gzmq_responder", __func__, __LINE__);
        g_zmq_source_close(&gzmq_responder);
    }
    
    if (NULL != zmq_context) {
        g_debug("[%s:%d] term zmq_context", __func__, __LINE__);
        zmq_ctx_term (zmq_context);
        zmq_context = NULL;
    }
    
    return (EXIT_SUCCESS);
}

