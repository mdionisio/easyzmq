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

#include <assert.h>

#include <string.h>

#include <sys/time.h>
#include <errno.h>

static void *zmq_context = NULL;
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
            g_main_loop_unref(loop);
            loop = NULL;
        }
        return G_SOURCE_REMOVE;
    }
}

const char default_address[] = "tcp://0.0.0.0:61000";

/*
 * 
 */
int main(int argc, char** argv) {
    const char *address = default_address;
    int rc __attribute__((unused));
    zmq_context = zmq_ctx_new ();
    
    loop = g_main_loop_new(NULL, FALSE);
    
    
    void *publisher = zmq_socket (zmq_context, ZMQ_PUB);
    
    {
        int linger = 1000;
        zmq_setsockopt(publisher, ZMQ_LINGER, &linger, sizeof(linger));
    }
    
    {
        int timeoutms = 1000;
        zmq_setsockopt(publisher, ZMQ_RCVTIMEO, &timeoutms, sizeof(timeoutms));
    }
    
    {
        int timeoutms = 1000;
        zmq_setsockopt(publisher, ZMQ_SNDTIMEO, &timeoutms, sizeof(timeoutms));
    }
  
    if (argc >= 2) {
        address = argv[1];
    }
    printf("address: %s\n", address);
    rc = zmq_bind (publisher, address);
    assert (rc == 0);
    
    GZmqSource *gzmq_publisher = g_zmq_source_new(g_main_loop_get_context(loop), zmq_context, publisher, 0);
    g_debug("[%s:%d] gzmq_subscriber created (%p)", __func__, __LINE__, gzmq_publisher);
    
    
    
    g_debug("[%s:%d] start mainloop", __func__, __LINE__);
    

    static typeoutdata_t data_timeout;
    data_timeout.gzmq_publisher = gzmq_publisher;
    
    g_timeout_add(1000, timeout_cb, &data_timeout);
        
    g_main_loop_run(loop);

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
    
    if (NULL != zmq_context) {
        g_debug("[%s:%d] term zmq_context", __func__, __LINE__);
        zmq_ctx_term (zmq_context);
        zmq_context = NULL;
    }
    
    return (EXIT_SUCCESS);
}

