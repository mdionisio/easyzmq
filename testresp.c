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
#include <glib-unix.h>

#include <assert.h>

#include <string.h>

#include <sys/time.h>
#include <errno.h>

static GMainLoop *loop = NULL;


typedef struct timeout_data {
    GZmqSource *gzmq_publisher;
} typeoutdata_t;

const char default_address[] = "tcp://0.0.0.0:5555";



static int counter = 0;

gboolean timeout_cb (gpointer user_data) {
    
    GZmqSource *zmq_source = (GZmqSource *)user_data;

    printf("answer.\n");
    gchar *datasend = g_strdup_printf("responce received: %d\n", counter);
    size_t len1 = strlen(datasend) + 1;
    gboolean ret __attribute__((unused)) = send_zmq_rep(zmq_source, (guint8*)datasend, len1);
    assert( ret == TRUE );
    
    g_free(datasend);
    counter++;

    return G_SOURCE_REMOVE;
}

gboolean resp_callback (GZmqSource *zmq_source, const guint8 *buf, int len, G_GNUC_UNUSED gboolean more, G_GNUC_UNUSED gpointer user_data) {
    const gchar* data = (const gchar*) buf;
    assert(zmq_source != NULL);
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
        printf("immediate\n");
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
    easy_zmq_init();
    
    loop = g_main_loop_new(NULL, FALSE);
     
    if (argc >= 2) {
        address = argv[1];
    }
    printf("address: %s\n", address);
    
    GZmqSource *gzmq_responder = g_zmq_source_rep_new(0, g_main_loop_get_context(loop));
    if (gzmq_responder != NULL) {
        g_debug("[%s:%d] gzmq_responder created (%p)", __func__, __LINE__, gzmq_responder);

        if (g_easyzmq_bind(gzmq_responder, address, NULL) == TRUE ) {
        
            if( recv_zmq_rep(gzmq_responder, resp_callback, NULL, NULL) == TRUE) {

                g_unix_signal_add (SIGINT, signal_handler, GINT_TO_POINTER (SIGINT));
                
                g_debug("[%s:%d] start mainloop", __func__, __LINE__);
                    
                g_main_loop_run(loop);

                recv_zmq_rep(gzmq_responder, NULL, NULL, NULL);
            } else {
                g_warning("[%s:%d] error resp", __func__, __LINE__);
            }
        } else {
            g_warning("[%s:%d] error bind", __func__, __LINE__);
        }
    } else {
        g_warning("[%s:%d] error creating socket", __func__, __LINE__);
    }

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
    
    easy_zmq_deinit();
    
    return (EXIT_SUCCESS);
}

