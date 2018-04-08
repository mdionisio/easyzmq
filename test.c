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

gboolean req_callback (GZmqSource *zmq_source, const guint8 *buf, int len, G_GNUC_UNUSED gboolean more, G_GNUC_UNUSED gpointer user_data) {
    const gchar* data = (const gchar*) buf;
    assert(buf != NULL);
    assert(zmq_source != NULL);
    assert(len > 0);
    struct timeval now;
    gettimeofday(&now, NULL);
    long int msec = (now.tv_sec * 1000L + now.tv_usec/1000);
    printf("%-10ld: req socket receive: '%s' len:%u\n", msec, data, (unsigned int) len);
    
    return TRUE;
}

typedef struct timeout_data {
    GZmqSource *gzmq_requester;
    GZmqSource *gzmq_publisher;
} typeoutdata_t;


gboolean timeout_cb (gpointer user_data) {
    typeoutdata_t *readdata = (typeoutdata_t *) user_data;
    
    GZmqSource *gzmq_requester = readdata->gzmq_requester;
    GZmqSource *gzmq_publisher = readdata->gzmq_publisher;
    
    static int counter = 0;
    struct timeval now;
    gettimeofday(&now, NULL);
    long int msec = (now.tv_sec * 1000L + now.tv_usec/1000);
    printf("%-10ld: timeout counter: %d\n", msec, counter);
    
    gchar *datasend = g_strdup_printf("req counter: %d\n", counter);
    size_t len = strlen(datasend) + 1;
    
    if (gzmq_requester != NULL) {
        assert( send_zmq_req(gzmq_requester, 1000, (guint8*)datasend, len, req_callback, NULL, NULL) == TRUE);
    }
    
    if (gzmq_publisher != NULL) {
        assert( send_zmq_pub(gzmq_publisher, (guint8*)datasend, len) == TRUE);
    }
    
    g_free(datasend);
    
    counter++;
    
    if (counter < 5) {
        return G_SOURCE_CONTINUE;
    } else {
        if (loop != NULL) {
            g_main_loop_quit(loop);
        }
        return G_SOURCE_REMOVE;
    }
}

gboolean resp_callback (GZmqSource *zmq_source, const guint8 *buf, int len, G_GNUC_UNUSED gboolean more, G_GNUC_UNUSED gpointer user_data) {
    const gchar* data = (const gchar*)buf;
    static int counter = 0;
    assert(zmq_source != NULL);
    assert(buf != NULL);
    assert(len > 0);
    struct timeval now;
    gettimeofday(&now, NULL);
    long int msec = (now.tv_sec * 1000L + now.tv_usec/1000);
    printf("%-10ld: resp socket receive: '%s' len:%u\n", msec, data, (unsigned int) len);
    
    gchar *datasend = g_strdup_printf("responce received: %d\n", counter);
    size_t len1 = strlen(datasend) + 1;
    gboolean ret __attribute__((unused)) = send_zmq_rep(zmq_source, (guint8*)datasend, len1);
    assert( ret == TRUE );
    
    g_free(datasend);
    counter++;
    
    return TRUE;
}

gboolean sub_callback (GZmqSource *zmq_source, const guint8 *buf, int len, G_GNUC_UNUSED gboolean more, G_GNUC_UNUSED gpointer user_data) {
    const gchar* data = (const gchar*)buf;
    static int counter = 0;
    assert(zmq_source != NULL);
    assert(buf != NULL);
    assert(len > 0);
    struct timeval now;
    gettimeofday(&now, NULL);
    long int msec = (now.tv_sec * 1000L + now.tv_usec/1000);
    printf("%-10ld: sub socket receive: '%s' len:%u\n", msec, data, (unsigned int) len);
    
    counter++;
    
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
int main(G_GNUC_UNUSED int argc, G_GNUC_UNUSED char** argv) {
    gboolean rc __attribute__((unused));
    easy_zmq_init();
    
    loop = g_main_loop_new(NULL, FALSE);
         
    GZmqSource *gzmq_respender = g_zmq_source_rep_new(1024*1024, g_main_loop_get_context(loop));
    assert (gzmq_respender != NULL);
    g_debug("[%s:%d] gzmq_respender created (%p)", __func__, __LINE__, gzmq_respender);
    
    rc = g_easyzmq_bind(gzmq_respender, "tcp://0.0.0.0:5555", NULL);
    assert (rc == TRUE);

    rc = recv_zmq_rep(gzmq_respender, resp_callback, NULL, NULL);
    assert (rc == TRUE);
       
    GZmqSource *gzmq_requester = g_zmq_source_req_new(1024*1024, g_main_loop_get_context(loop));
    assert (gzmq_requester != NULL);
    g_debug("[%s:%d] gzmq_requester created (%p)", __func__, __LINE__, gzmq_requester);
    
    rc = g_easyzmq_connect(gzmq_requester, "tcp://localhost:5555", NULL);
    assert (rc == TRUE);
    
    GZmqSource *gzmq_subscriber = g_zmq_source_sub_new(1024*1024, g_main_loop_get_context(loop));
    g_debug("[%s:%d] gzmq_subscriber created (%p)", __func__, __LINE__, gzmq_subscriber);

    rc = g_easyzmq_connect(gzmq_subscriber, "tcp://localhost:5556", NULL);
    assert (rc == TRUE);

    rc = recv_zmq_sub(gzmq_subscriber, sub_callback, NULL, NULL);
    assert (rc == TRUE);

    rc = subscribe_zmq_sub(gzmq_subscriber, (guint8*)"", 0, NULL);
    assert (rc == TRUE);
        
    GZmqSource *gzmq_publisher = g_zmq_source_pub_new(g_main_loop_get_context(loop));
    g_debug("[%s:%d] gzmq_subscriber created (%p)", __func__, __LINE__, gzmq_publisher);   

    rc = g_easyzmq_bind(gzmq_publisher, "tcp://0.0.0.0:5556", NULL);
    assert (rc == TRUE);
    
    g_debug("[%s:%d] start mainloop", __func__, __LINE__);
    

    static typeoutdata_t data_timeout;
    data_timeout.gzmq_requester = gzmq_requester;
    data_timeout.gzmq_publisher = gzmq_publisher;
    //data_timeout.gzmq_publisher = NULL;
    
    g_timeout_add(1000, timeout_cb, &data_timeout);

    g_unix_signal_add (SIGINT, signal_handler, GINT_TO_POINTER (SIGINT));
        
    g_main_loop_run(loop);

    g_debug("[%s:%d] stop mainloop", __func__, __LINE__);
    
    if (loop != NULL) {
        g_debug("[%s:%d] unref mainloop", __func__, __LINE__);
        g_main_loop_unref(loop);
        loop = NULL;
    }
    
    if (gzmq_respender != NULL) {
        g_debug("[%s:%d] destroy gzmq_respender", __func__, __LINE__);
        g_zmq_source_close(&gzmq_respender);
    }
    
    if (gzmq_requester != NULL) {
        g_debug("[%s:%d] destroy gzmq_requester", __func__, __LINE__);
        g_zmq_source_close(&gzmq_requester);
    }
    
    if (gzmq_publisher != NULL) {
        g_debug("[%s:%d] destroy gzmq_publisher", __func__, __LINE__);
        g_zmq_source_close(&gzmq_publisher);
    }
    
    if (gzmq_subscriber != NULL) {
        g_debug("[%s:%d] destroy gzmq_subscriber", __func__, __LINE__);
        g_zmq_source_close(&gzmq_subscriber);
    }

    easy_zmq_deinit();
        
    return (EXIT_SUCCESS);
}

