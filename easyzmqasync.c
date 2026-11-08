/* async (GAsyncResult) wrapper of send_zmq_req, used by the vala binding */

#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

#include "easyzmq.h"
#include <gio/gio.h>
#include <glib-object.h>
#include <glib.h>

// uncomment next line if you need extra debug
// #define EASYZMQ_DBG

#ifdef EASYZMQ_DBG
#define DBG(x, ...) g_debug(x, ##__VA_ARGS__)
#else
#define DBG(x, ...)
#endif

typedef struct _SendReq_asyncData SendReq_asyncData;

struct _SendReq_asyncData {
    GZmqSource *req;
    const guint8 *bufout; // valid only during the completion callback
    gint bufout_length;
    gboolean more;
    gboolean completed;
};

static void SendReq_async_data_free(gpointer _data) {
    g_assert(_data != NULL);
    SendReq_asyncData *_data_ = _data;
    DBG("async end: %p", _data_);
    g_zmq_source_unref(_data_->req);
    g_slice_free(SendReq_asyncData, _data_);
}

static gboolean _gzmqsourcecallback_easy(GZmqSource *zmq_source, const guint8 *buf, int buf_length, gboolean more, gpointer self) {
    g_assert(self != NULL);
    g_return_val_if_fail(zmq_source != NULL, FALSE);

    GSimpleAsyncResult *const async_result = G_SIMPLE_ASYNC_RESULT(self);
    SendReq_asyncData *const _data_ = g_simple_async_result_get_op_res_gpointer(async_result);
    g_assert(_data_ != NULL);

    if (_data_->completed) {
        // the async call is completed with the first frame of the answer
        return TRUE;
    }
    _data_->completed = TRUE;
    if (buf_length != 0) {
        _data_->bufout = buf;
        _data_->bufout_length = buf_length;
    }
    _data_->more = more;

    // the async_result reference is owned by the request (released by the notify)
    g_simple_async_result_complete(async_result);
    _data_->bufout = NULL;
    _data_->bufout_length = 0;

    return TRUE;
}

static GSimpleAsyncResult *_send_zmq_req_async_start(GZmqSource *req, GAsyncReadyCallback _callback_, gpointer _user_data_) {
    g_assert(req != NULL);
    g_assert(_callback_ != NULL);

    SendReq_asyncData *_data_ = g_slice_new0(SendReq_asyncData);
    DBG("async start: %p", _data_);

    GSimpleAsyncResult *const async_result = g_simple_async_result_new(NULL, _callback_, _user_data_, (gpointer)send_zmq_req_async);
    g_assert(async_result != NULL);

    _data_->req = g_zmq_source_ref(req);
    _data_->bufout = NULL;
    _data_->bufout_length = 0;
    _data_->more = FALSE;
    _data_->completed = FALSE;
    g_simple_async_result_set_op_res_gpointer(async_result, _data_, SendReq_async_data_free);

    return async_result;
}

static void _send_zmq_req_async_sent(GSimpleAsyncResult *async_result, gboolean sendret) {
    if (!sendret) {
        // immediately continue the async call with error answer
        g_simple_async_result_complete_in_idle(async_result);
    }
    g_object_unref(async_result);
}

void send_zmq_req_async(GZmqSource *req, guint timeoutms, const guint8 *buf, int buf_length, GAsyncReadyCallback _callback_, gpointer _user_data_) {
    GSimpleAsyncResult *const async_result = _send_zmq_req_async_start(req, _callback_, _user_data_);
    const gboolean sendret = send_zmq_req(req, timeoutms, buf, buf_length, _gzmqsourcecallback_easy, g_object_ref(async_result), g_object_unref);
    _send_zmq_req_async_sent(async_result, sendret);
}

void send_zmq_req_take_async(GZmqSource *req, guint timeoutms, guint8 *buf, int buf_length, GAsyncReadyCallback _callback_, gpointer _user_data_) {
    GSimpleAsyncResult *const async_result = _send_zmq_req_async_start(req, _callback_, _user_data_);
    // buf is owned (allocated with g_malloc as the vala owned array)
    const gboolean sendret = send_zmq_req_take(req, timeoutms, buf, buf_length, g_free, _gzmqsourcecallback_easy, g_object_ref(async_result), g_object_unref);
    _send_zmq_req_async_sent(async_result, sendret);
}

void send_zmq_req_finish(GZmqSource *req, GAsyncResult *_res_, const guint8 **bufout, int *bufout_length, gboolean *more) {
    g_assert(req != NULL);
    g_assert(_res_ != NULL);

    SendReq_asyncData *_data_ = g_simple_async_result_get_op_res_gpointer(G_SIMPLE_ASYNC_RESULT(_res_));
    g_assert(_data_ != NULL);

    if (bufout) {
        *bufout = _data_->bufout;
    }
    if (bufout_length) {
        *bufout_length = _data_->bufout_length;
    }
    if (more) {
        *more = _data_->more;
    }
}
