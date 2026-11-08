# easyzmq

ZeroMQ sockets integrated in the GLib main loop, for C and Vala.

Every socket is a `GSource` (`GZmqSource`) attached to a `GMainContext`: messages are
received through callbacks (or async/await in Vala), sends never block the main loop.

Supported sockets: REQ, REP, ROUTER, DEALER, PUB, SUB, XPUB, XSUB, PUSH, PULL, PAIR.

## Build and install

Requirements: libzmq >= 4.0, glib/gio >= 2.44, meson. `valac` is optional (vala examples).

```sh
meson setup build
ninja -C build
meson test -C build      # automatic tests (inproc only, no network)
ninja -C build install   # library, easyzmq.h, easyzmq.pc, easyzmq.vapi
```

Options: `-Dtests=false`, `-Dexamples=false`, `-Dexamples_vala=disabled`.

Use from another meson project (installed or as subproject):

```meson
easyzmq_dep = dependency('easyzmq', fallback : ['easyzmq', 'easyzmq_dep'])
```

Vala: `valac --pkg easyzmq ...` (the vapi is installed in `share/vala/vapi`).

## Quick start (C)

```c
#include <easyzmq.h>

static gboolean on_request(GZmqSource *rep, const guint8 *buf, int len, gboolean more, gpointer user_data) {
    send_zmq_rep(rep, buf, len); // echo
    return G_SOURCE_CONTINUE;
}

int main(void) {
    easy_zmq_init();
    GMainLoop *loop = g_main_loop_new(NULL, FALSE);

    GZmqSource *rep = g_zmq_source_rep_new(0, NULL);
    g_easyzmq_bind(rep, "tcp://*:5555", NULL);
    recv_zmq_rep(rep, on_request, NULL, NULL);

    g_main_loop_run(loop);

    g_zmq_source_close(&rep);
    g_main_loop_unref(loop);
    easy_zmq_deinit();
}
```

## Quick start (Vala, async)

```vala
async void client () {
    var req = new EasyZmq.ZmqSource.Req ();
    try {
        req.Connect ("tcp://127.0.0.1:5555");
        Bytes answer = yield req.Request (new Bytes ("hello".data), 2000);
    } catch (IOError.TIMED_OUT e) {
        // no answer in 2 s
    } catch (Error e) {
        // EasyZmq.Error (send error, code = zmq errno) or IOError.CANCELLED
    }
}

async void server (EasyZmq.ZmqSource rep, Cancellable stop) throws Error {
    while (true) {
        Bytes request = yield rep.Receive (stop);
        rep.SendRep (request.get_data ());
    }
}
```

See `testasyncvala.vala` (self contained, inproc), `testrepasyncvala.vala`,
`testreqasyncvala.vala`, `testsubasyncvala.vala`.

## API overview

| Area | Functions |
|---|---|
| init | `easy_zmq_init`, `easy_zmq_init_with_context`, `easy_zmq_deinit` (reference counted) |
| create | `g_zmq_source_<type>_new`, `g_zmq_source_new_from_socket` (socket created by you, options already set) |
| lifetime | `g_zmq_source_ref`, `g_zmq_source_unref`, `g_zmq_source_close` |
| endpoints | `g_easyzmq_bind`, `g_easyzmq_unbind`, `g_easyzmq_connect`, `g_easyzmq_disconnect` |
| options | `g_easyzmq_set_option_int/int64/bytes/string`, `g_easyzmq_get_option_*`, `g_easyzmq_get_socket` |
| REQ | `send_zmq_req`, `send_zmq_req_take`, `send_zmq_req_bytes`, `g_easyzmq_request_async` |
| REP | `recv_zmq_rep`, `send_zmq_rep*` |
| ROUTER | `recv_zmq_router`, `send_zmq_router*`, `destroy_identity` |
| PUB / SUB | `send_zmq_pub*`, `subscribe_zmq_sub`, `unsubscribe_zmq_sub`, `recv_zmq_sub` |
| generic | `g_easyzmq_recv`, `g_easyzmq_send`, `g_easyzmq_send_take`, `g_easyzmq_send_more` (multipart) |
| GBytes | `g_easyzmq_recv_bytes`, `g_easyzmq_send_bytes`, `g_easyzmq_get_current_bytes` (zero copy) |
| async | `g_easyzmq_request_async`, `g_easyzmq_receive_async` (+ `_finish`, `_multipart_finish`) |
| flow | `g_easyzmq_set_paused`, `g_easyzmq_set_blocking_send` |
| monitor | `get_zmq_socket_monitor_full` (all events, user_data), `get_zmq_socket_monitor` |

## Rules to know

**Buffers and ownership**
- `buf` passed to a receive callback is valid only during the callback. To keep the data use
  `g_easyzmq_get_current_bytes()` (or the GBytes/async API): without pre-allocated memory
  (`max_mem_size == 0`) the data is not copied.
- `*_take` functions take the ownership of the buffer: it is released with `destroy_data`, also
  when the send fails.
- `user_data` passed with a callback is released (`notify`) when the callback is replaced or the
  source destroyed. For `send_zmq_req*` it is released after the answer, the timeout or a send error.

**Sending**
- Sends are not blocking (`ZMQ_DONTWAIT`): if a message can not be queued (no peer, high water
  mark) the function returns `FALSE`. `g_easyzmq_set_blocking_send(source, TRUE)` restores the
  blocking behaviour.

**REQ**
- One request at a time per socket: use more REQ sockets for parallel requests.
- With `timeoutms > 0` the socket is relaxed and correlated (`ZMQ_REQ_RELAXED`,
  `ZMQ_REQ_CORRELATE`): after a timeout a new request can be sent and a late answer is discarded.
  With `timeoutms == 0` the socket is a strict REQ: a new request fails until the answer arrives.
- A failed send does not touch the request still pending.

**ROUTER**
- The ROUTER source expects REQ peers (envelope with empty delimiter). A DEALER talking to it
  has to send the empty frame first (`g_easyzmq_send_more(dealer, NULL, 0)`), messages without
  delimiter are discarded.
- The `identity` passed to the callback is owned by you after the last frame: release it with
  `send_zmq_router*` (answer) or `destroy_identity`.

**Async receive**
- More `Receive` can be pending, they are completed in order. When nobody is waiting the source
  is paused and the messages stay queued in zmq.
- Registering a callback (`recv_zmq_*`, `g_easyzmq_recv*`) on the same source closes the pending
  receives with `G_IO_ERROR_CLOSED`.

**Threads**
- A source has to be used only from the thread running its `GMainContext`.

## Examples

| Program | What |
|---|---|
| `testzmqasyncvala` | async API, self contained (inproc), also a test |
| `testzmqrepasyncvala` / `testzmqreqasyncvala` | async REP server / REQ client over tcp |
| `testzmqsubasyncvala` | async multipart subscriber (run with `testzmqpub`) |
| `testzmqreqmonitorvala` | REQ with monitor (closure, all events) |
| `testzmqresp`, `testzmqreq`, `testzmqreqtimeout`, `testzmqrouterresp` | C request/reply |
| `testzmqpub`, `testzmqpubmon`, `testzmqsub`, `testzmqpair1/2` | C pub/sub and pair |
