/*
 * File:   easyzmqtask.c
 * Author: Michele Dionisio
 *
 * GTask based async API (GError, GCancellable, GBytes)
 */

#include "easyzmq.h"
#include "easyzmq-private.h"

#include <gio/gio.h>
#include <glib.h>

// ---- common

typedef struct {
    GPtrArray *frames; // GBytes of the received message
    GSource *cancel_source;
    gboolean done;
} task_data_t;

static void task_data_free(gpointer data) {
    task_data_t *const tdata = (task_data_t *)data;
    g_assert(tdata != NULL);
    if (tdata->cancel_source != NULL) {
        g_source_destroy(tdata->cancel_source);
        g_source_unref(tdata->cancel_source);
        tdata->cancel_source = NULL;
    }
    if (tdata->frames != NULL) {
        g_ptr_array_unref(tdata->frames);
        tdata->frames = NULL;
    }
    g_slice_free(task_data_t, tdata);
}

static task_data_t *task_data_new(GTask *task) {
    task_data_t *const tdata = g_slice_new0(task_data_t);
    tdata->frames = g_ptr_array_new_with_free_func((GDestroyNotify)g_bytes_unref);
    tdata->done = FALSE;
    g_task_set_task_data(task, tdata, task_data_free);
    return tdata;
}

// complete the task only once (answer, error, timeout and cancel can race)
static gboolean task_complete_frames(GTask *task) {
    task_data_t *const tdata = g_task_get_task_data(task);
    if (tdata->done) {
        return FALSE;
    }
    tdata->done = TRUE;
    GPtrArray *const frames = tdata->frames;
    tdata->frames = NULL;
    g_task_return_pointer(task, frames, (GDestroyNotify)g_ptr_array_unref);
    return TRUE;
}

static gboolean task_complete_error(GTask *task, GQuark domain, gint code, const gchar *message) {
    task_data_t *const tdata = g_task_get_task_data(task);
    if (tdata->done) {
        return FALSE;
    }
    tdata->done = TRUE;
    g_task_return_new_error(task, domain, code, "%s", message);
    return TRUE;
}

static GBytes *frames_to_bytes(GPtrArray *frames) {
    g_assert(frames != NULL);
    GBytes *ret = NULL;
    if (frames->len == 1) {
        ret = g_bytes_ref(g_ptr_array_index(frames, 0));
    } else {
        // multipart message: all the frames concatenated
        GByteArray *const all = g_byte_array_new();
        for (guint i = 0; i < frames->len; i++) {
            gsize size = 0;
            const guint8 *const data = g_bytes_get_data(g_ptr_array_index(frames, i), &size);
            g_byte_array_append(all, data, (guint)size);
        }
        ret = g_byte_array_free_to_bytes(all);
    }
    g_ptr_array_unref(frames);
    return ret;
}

typedef void (*task_cancelled_func)(GTask *task);

static gboolean task_cancelled_cb(GCancellable *cancellable, gpointer user_data) {
    (void)cancellable;
    GTask *const task = G_TASK(user_data);
    task_data_t *const tdata = g_task_get_task_data(task);
    task_cancelled_func const on_cancel = (task_cancelled_func)g_object_get_data(G_OBJECT(task), "easyzmq-on-cancel");
    if (!tdata->done) {
        g_object_ref(task);
        if (on_cancel != NULL) {
            on_cancel(task);
        }
        task_complete_error(task, G_IO_ERROR, G_IO_ERROR_CANCELLED, "operation cancelled");
        g_object_unref(task);
    }
    return G_SOURCE_REMOVE;
}

// the cancellation is managed in the main context of the task (never from another thread)
static void task_watch_cancellable(GTask *task, GCancellable *cancellable, task_cancelled_func on_cancel) {
    if (cancellable == NULL) {
        return;
    }
    task_data_t *const tdata = g_task_get_task_data(task);
    g_object_set_data(G_OBJECT(task), "easyzmq-on-cancel", (gpointer)on_cancel);
    tdata->cancel_source = g_cancellable_source_new(cancellable);
    // the task owns the cancel source: no reference cycle (destroyed with task data)
    g_source_set_callback(tdata->cancel_source, (GSourceFunc)(void *)task_cancelled_cb, task, NULL);
    g_source_attach(tdata->cancel_source, g_task_get_context(task));
}

// ---- REQ

static gboolean request_cb(GZmqSource *zmq_source, const guint8 *buf, int len, gboolean more, gpointer user_data) {
    (void)len;
    GTask *const task = G_TASK(user_data);
    task_data_t *const tdata = g_task_get_task_data(task);

    if (tdata->done) {
        return G_SOURCE_CONTINUE; // already cancelled
    }
    if (buf == NULL) {
        task_complete_error(task, G_IO_ERROR, G_IO_ERROR_TIMED_OUT, "no answer received (timeout or receive error)");
        return G_SOURCE_CONTINUE;
    }
    g_ptr_array_add(tdata->frames, g_easyzmq_get_current_bytes(zmq_source));
    if (!more) {
        task_complete_frames(task);
    }
    return G_SOURCE_CONTINUE;
}

void g_easyzmq_request_async(GZmqSource *req, GBytes *request, guint timeoutms, GCancellable *cancellable, GAsyncReadyCallback callback, gpointer user_data) {
    g_return_if_fail(req != NULL);
    g_return_if_fail(request != NULL);
    g_return_if_fail(g_easyzmq_get_socket_type(req) == EASY_ZMQ_REQ);

    GTask *const task = g_task_new(NULL, cancellable, callback, user_data);
    g_task_set_source_tag(task, g_easyzmq_request_async);
    task_data_new(task);

    if (g_task_return_error_if_cancelled(task)) {
        g_object_unref(task);
        return;
    }

    const gsize size = g_bytes_get_size(request);
    if ((size == 0) || (size > (gsize)G_MAXINT)) {
        task_complete_error(task, G_IO_ERROR, G_IO_ERROR_INVALID_ARGUMENT, "invalid request size");
        g_object_unref(task);
        return;
    }

    // the request owns a reference of the task (released by the notify)
    const guint8 *const data = g_bytes_get_data(request, NULL);
    if (send_zmq_req(req, timeoutms, data, (int)size, request_cb, g_object_ref(task), g_object_unref)) {
        task_watch_cancellable(task, cancellable, NULL);
    } else {
        const int zerrno = zmq_errno();
        gchar *const message = g_strdup_printf("send error: %s", zmq_strerror(zerrno));
        task_complete_error(task, EASY_ZMQ_ERROR, zerrno, message);
        g_free(message);
    }
    g_object_unref(task);
}

GPtrArray *g_easyzmq_request_multipart_finish(GZmqSource *req, GAsyncResult *result, GError **error) {
    (void)req;
    g_return_val_if_fail(g_task_is_valid(result, NULL), NULL);
    return g_task_propagate_pointer(G_TASK(result), error);
}

GBytes *g_easyzmq_request_finish(GZmqSource *req, GAsyncResult *result, GError **error) {
    GPtrArray *const frames = g_easyzmq_request_multipart_finish(req, result, error);
    return (frames != NULL) ? frames_to_bytes(frames) : NULL;
}

// ---- receive (SUB, PAIR, REP, ...)

typedef struct {
    GZmqSource *source; // not owned (the state is owned by the source)
    GQueue tasks;       // waiting receive
    GQueue ready;       // received messages (GPtrArray) with nobody waiting
    GPtrArray *frames;  // message under receive
} recv_state_t;

static void recv_state_free(gpointer data) {
    recv_state_t *const state = (recv_state_t *)data;
    GTask *task = NULL;
    // another callback has been registered or the source is destroyed
    while ((task = g_queue_pop_head(&state->tasks)) != NULL) {
        task_complete_error(task, G_IO_ERROR, G_IO_ERROR_CLOSED, "receive closed");
        g_object_unref(task);
    }
    g_queue_clear_full(&state->ready, (GDestroyNotify)g_ptr_array_unref);
    if (state->frames != NULL) {
        g_ptr_array_unref(state->frames);
    }
    if (state->source->async_recv == state) {
        state->source->async_recv = NULL;
        state->source->paused = FALSE;
    }
    g_slice_free(recv_state_t, state);
}

static void recv_update_pause(recv_state_t *state) {
    // without receivers the messages stay queued in zmq (high water mark)
    g_easyzmq_set_paused(state->source, g_queue_is_empty(&state->tasks));
}

static gboolean recv_cb(GZmqSource *zmq_source, const guint8 *buf, int len, gboolean more, gpointer user_data) {
    (void)len;
    recv_state_t *const state = (recv_state_t *)user_data;

    if (buf == NULL) {
        GTask *const task = g_queue_pop_head(&state->tasks);
        if (task != NULL) {
            task_complete_error(task, G_IO_ERROR, G_IO_ERROR_FAILED, "receive error");
            g_object_unref(task);
        }
        if (state->frames != NULL) {
            g_ptr_array_unref(state->frames);
            state->frames = NULL;
        }
        recv_update_pause(state);
        return G_SOURCE_CONTINUE;
    }

    if (state->frames == NULL) {
        state->frames = g_ptr_array_new_with_free_func((GDestroyNotify)g_bytes_unref);
    }
    g_ptr_array_add(state->frames, g_easyzmq_get_current_bytes(zmq_source));

    if (!more) {
        GPtrArray *const frames = state->frames;
        state->frames = NULL;
        GTask *const task = g_queue_pop_head(&state->tasks);
        if (task != NULL) {
            task_data_t *const tdata = g_task_get_task_data(task);
            g_ptr_array_unref(tdata->frames);
            tdata->frames = frames;
            task_complete_frames(task);
            g_object_unref(task);
        } else {
            g_queue_push_tail(&state->ready, frames);
        }
        recv_update_pause(state);
    }
    return G_SOURCE_CONTINUE;
}

static void recv_task_cancelled(GTask *task) {
    GZmqSource *const source = g_object_get_data(G_OBJECT(task), "easyzmq-source");
    recv_state_t *const state = (source != NULL) ? source->async_recv : NULL;
    if ((state != NULL) && g_queue_remove(&state->tasks, task)) {
        g_object_unref(task);
        recv_update_pause(state);
    }
}

void g_easyzmq_receive_async(GZmqSource *source, GCancellable *cancellable, GAsyncReadyCallback callback, gpointer user_data) {
    g_return_if_fail(source != NULL);
    const easyzmq_socket_t type = g_easyzmq_get_socket_type(source);
    g_return_if_fail((type != EASY_ZMQ_REQ) && (type != EASY_ZMQ_PUB) && (type != EASY_ZMQ_ROUTER));

    GTask *const task = g_task_new(NULL, cancellable, callback, user_data);
    g_task_set_source_tag(task, g_easyzmq_receive_async);
    task_data_new(task);

    if (g_task_return_error_if_cancelled(task)) {
        g_object_unref(task);
        return;
    }

    recv_state_t *state = (recv_state_t *)source->async_recv;
    if (state == NULL) {
        state = g_slice_new0(recv_state_t);
        state->source = source;
        g_queue_init(&state->tasks);
        g_queue_init(&state->ready);
        g_source_set_callback((GSource *)source, (GSourceFunc)(void *)recv_cb, state, recv_state_free);
        source->async_recv = state;
    }

    GPtrArray *const frames = g_queue_pop_head(&state->ready);
    if (frames != NULL) {
        // message already received
        task_data_t *const tdata = g_task_get_task_data(task);
        g_ptr_array_unref(tdata->frames);
        tdata->frames = frames;
        task_complete_frames(task);
        g_object_unref(task);
        return;
    }

    g_object_set_data(G_OBJECT(task), "easyzmq-source", source);
    g_queue_push_tail(&state->tasks, task); // reference owned by the queue
    task_watch_cancellable(task, cancellable, recv_task_cancelled);
    recv_update_pause(state);
}

GPtrArray *g_easyzmq_receive_multipart_finish(GZmqSource *source, GAsyncResult *result, GError **error) {
    (void)source;
    g_return_val_if_fail(g_task_is_valid(result, NULL), NULL);
    return g_task_propagate_pointer(G_TASK(result), error);
}

GBytes *g_easyzmq_receive_finish(GZmqSource *source, GAsyncResult *result, GError **error) {
    GPtrArray *const frames = g_easyzmq_receive_multipart_finish(source, result, error);
    return (frames != NULL) ? frames_to_bytes(frames) : NULL;
}
