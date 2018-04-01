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


gboolean sub_callback (GZmqSource *zmq_source, const void *buf, size_t len, gpointer user_data) {
    static int counter = 0;
    assert(buf != NULL);
    assert(len > 0);
    struct timeval now;
    gettimeofday(&now, NULL);
    long int msec = (now.tv_sec * 1000L + now.tv_usec/1000);
    printf("%-10ld: sub socket receive: len:%u\n", msec, (unsigned int) len);
    
    counter++;
    
    return TRUE;
}

const char default_address[] = "tcp://127.0.0.1:61000";

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
    
    GZmqSource *gzmq_subscriber = g_zmq_source_sub_new(128*1024, g_main_loop_get_context(loop));
    if (gzmq_subscriber != NULL) {
        g_debug("[%s:%d] subscriber created (%p)", __func__, __LINE__, gzmq_subscriber);

        if (g_easyzmq_connect(gzmq_subscriber, address, NULL) == TRUE ) {
        
            if( recv_zmq_sub(gzmq_subscriber, "", sub_callback, NULL, NULL) == TRUE) {

                g_unix_signal_add (SIGINT, signal_handler, GINT_TO_POINTER (SIGINT));
                
                g_debug("[%s:%d] start mainloop", __func__, __LINE__);
                    
                g_main_loop_run(loop);
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
    
    if (gzmq_subscriber != NULL) {
        g_debug("[%s:%d] destroy gzmq_subscriber", __func__, __LINE__);
        g_zmq_source_close(&gzmq_subscriber);
    }
    
    easy_zmq_deinit();
    
    return (EXIT_SUCCESS);
}

