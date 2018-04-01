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

const char default_address[] = "tcp://127.0.0.1:5555";
const char *address = default_address;

typedef struct timeout_data {
    GZmqSource *gzmq_requester;
} typeoutdata_t;

static typeoutdata_t data_timeout = { NULL };

gboolean init_requester(gpointer user_data) {
    int rc __attribute__((unused));

    if (data_timeout.gzmq_requester != NULL) {
        g_zmq_source_unref(data_timeout.gzmq_requester);
        data_timeout.gzmq_requester = NULL;
    }

    GZmqSource *gzmq_requester = g_zmq_source_req_new(0, g_main_loop_get_context(loop));
    if (gzmq_requester != NULL) {
        if (g_easyzmq_connect(gzmq_requester, address, NULL)) {
            g_debug("[%s:%d] gzmq_requester created (%p)", __func__, __LINE__, gzmq_requester);

            data_timeout.gzmq_requester = gzmq_requester;
        } else {
            g_zmq_source_unref(gzmq_requester);
        }
    }

    return G_SOURCE_REMOVE;
}

gboolean req_callback (GZmqSource *zmq_source, const void *buf, size_t len, gpointer user_data) {
    struct timeval now;
        gettimeofday(&now, NULL);
        long int msec = (now.tv_sec * 1000L + now.tv_usec/1000);
    if ((buf != NULL) && (len > 0)) {
        const gchar* data = buf;
        printf("%-10ld: req socket receive: '%s' len:%u\n", msec, data, (unsigned int) len);
    } else {
        printf("%-10ld: req socket timeout\n", msec);
        //g_idle_add(init_requester, NULL);
        init_requester(NULL);
    }
    
    return TRUE;
}

void destroy_userdata (gpointer data) {
    char * cdata = (char*)data;
    printf("request to destroy user data: %s\n", cdata);
    g_free(data);
}

void destroy_data (gpointer data) {
    char * cdata = (char*)data;
    printf("request to destroy data: %s\n", cdata);
    g_free(data);
}

gboolean timeout_cb (gpointer user_data) {
    GZmqSource *gzmq_requester = data_timeout.gzmq_requester;
    
    static int counter = 0;
    struct timeval now;
    gettimeofday(&now, NULL);
    long int msec = (now.tv_sec * 1000L + now.tv_usec/1000);
    printf("%-10ld: send request counter: %d\n", msec, counter);
    
    if (gzmq_requester != NULL) {
        gchar *datasend = g_strdup_printf("req counter: %d\n", counter);
        size_t len = strlen(datasend) + 1;

        //assert( send_zmq_req(gzmq_requester, 1000, datasend, len, req_callback, NULL, NULL) == TRUE);
        gchar *userdata = g_strdup_printf("%d\n", counter);
        if ( send_zmq_req_take(gzmq_requester, 1000, datasend, len, destroy_data, req_callback, userdata, destroy_userdata) != TRUE) {
            printf("req socket send failed\n");
        }
    }
    
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
    address = default_address;
    
    easy_zmq_init();
    
    loop = g_main_loop_new(NULL, FALSE);

    if (argc >= 2) {
        address = argv[1];
    }
    
    init_requester(NULL);
    
    g_debug("[%s:%d] start mainloop", __func__, __LINE__);    
    
    g_timeout_add(2000, timeout_cb, NULL);

    g_unix_signal_add (SIGINT, signal_handler, GINT_TO_POINTER (SIGINT));
        
    g_main_loop_run(loop);

    g_debug("[%s:%d] stop mainloop", __func__, __LINE__);
    
    if (loop != NULL) {
        g_debug("[%s:%d] unref mainloop", __func__, __LINE__);
        g_main_loop_unref(loop);
        loop = NULL;
    }
    
    if (data_timeout.gzmq_requester != NULL) {
        g_debug("[%s:%d] destroy gzmq_requester", __func__, __LINE__);
        g_zmq_source_close(&data_timeout.gzmq_requester);
        data_timeout.gzmq_requester = NULL;
    }
    
    easy_zmq_deinit();
    
    return (EXIT_SUCCESS);
}

