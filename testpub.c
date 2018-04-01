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


gboolean timeout_cb (gpointer user_data) {
    typeoutdata_t *readdata = (typeoutdata_t *) user_data;
    
    GZmqSource *gzmq_publisher = readdata->gzmq_publisher;
    
    static int counter = 0;
    struct timeval now;
    gettimeofday(&now, NULL);
    long int msec = (now.tv_sec * 1000L + now.tv_usec/1000);
    printf("%-10ld: timeout counter: %d\n", msec, counter);
    
    gchar *datasend = g_strdup_printf("req counter: %d\n", counter);
    size_t len = strlen(datasend) + 1;
    
    if (gzmq_publisher != NULL) {
        send_zmq_pub(gzmq_publisher, datasend, len);
    }
    
    g_free(datasend);
    
    counter++;
    
    if (counter < 15) {
        return G_SOURCE_CONTINUE;
    } else {
        if (loop != NULL) {
            g_main_loop_quit(loop);
        }
        return G_SOURCE_REMOVE;
    }
}

const char default_address[] = "tcp://0.0.0.0:61000";

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
    
    GZmqSource *gzmq_publisher = g_zmq_source_pub_new(g_main_loop_get_context(loop));
    if (gzmq_publisher != NULL) {
        g_debug("[%s:%d] publischer created (%p)", __func__, __LINE__, gzmq_publisher);

        if (g_easyzmq_bind(gzmq_publisher, address, NULL) == TRUE ) {
            g_unix_signal_add (SIGINT, signal_handler, GINT_TO_POINTER (SIGINT));

            static typeoutdata_t data_timeout;
            data_timeout.gzmq_publisher = gzmq_publisher;
            
            g_timeout_add(1000, timeout_cb, &data_timeout);

            g_unix_signal_add (SIGINT, signal_handler, GINT_TO_POINTER (SIGINT));
            
            g_debug("[%s:%d] start mainloop", __func__, __LINE__);
                
            g_main_loop_run(loop);
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
    
    if (gzmq_publisher != NULL) {
        g_debug("[%s:%d] destroy gzmq_publisher", __func__, __LINE__);
        g_zmq_source_close(&gzmq_publisher);
    }
    
    easy_zmq_deinit();
    
    return (EXIT_SUCCESS);
}

