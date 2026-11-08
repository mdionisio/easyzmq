[CCode (cheader_filename = "easyzmq.h")]
namespace EasyZmq {
    // the GError code is the zmq errno value
    [CCode (cname = "int", cprefix = "", cheader_filename = "errno.h,zmq.h,easyzmq.h")]
    public errordomain Error {
        EINVAL,
        EPROTONOSUPPORT,
        ENOCOMPATPROTO,
        ETERM,
        ENOTSOCK,
        EMTHREAD,
        EFAULT,
        EMFILE,
        EADDRINUSE,
        EADDRNOTAVAIL,
        ENODEV,
        EINTR,
        EAGAIN,
        EFSM,
        EHOSTUNREACH;
        [CCode (cname = "easyzmq_error_quark")]
        public static GLib.Quark quark ();
    }

    [CCode (cname = "easyzmq_monitor_event_t", cprefix = "EASYZMQ_EVENT_", has_type_id = false)]
    public enum monitor_event {
        UNKNOWN,
        CONNECTED,
        CONNECT_DELAYED,
        CONNECT_RETRIED,
        LISTENING,
        BIND_FAILED,
        ACCEPTED,
        ACCEPT_FAILED,
        CLOSED,
        CLOSE_FAILED,
        DISCONNECTED,
        MONITOR_STOPPED;
    }

    [CCode (cname = "easyzmq_socket_t", cprefix = "EASY_ZMQ_", has_type_id = false)]
    public enum SocketType {
        PUB,
        SUB,
        REQ,
        REP,
        PAIR,
        ROUTER,
        UNKNOWN
    }

    [CCode (cname = "easy_zmq_init")]
    public void Init();

    [CCode (cname = "easy_zmq_init_with_context")]
    public void InitWithContext(void* zmq_context);

    [CCode (cname = "easy_zmq_deinit")]
    public void Deinit();

    [CCode (cname = "get_zmq_context")]
    public void* get_zmq_context();

    [Compact]
    [CCode (cname = "GZmqSource", ref_function = "g_zmq_source_ref", unref_function = "g_zmq_source_unref")]
    public class ZmqSource {
        [CCode (cname = "g_zmq_source_req_new")]
        public ZmqSource.Req (size_t max_size = 0, GLib.MainContext ? gcontext = null);

        [CCode (cname = "g_zmq_source_rep_new")]
        public ZmqSource.Rep (size_t max_size = 0, GLib.MainContext ? gcontext = null);

        [CCode (cname = "g_zmq_source_pub_new")]
        public ZmqSource.Pub (GLib.MainContext ? gcontext = null);

        [CCode (cname = "g_zmq_source_sub_new")]
        public ZmqSource.Sub (size_t max_size = 0, GLib.MainContext ? gcontext = null);

        [CCode (cname = "g_zmq_source_pair_new")]
        public ZmqSource.Pair (size_t max_size = 0, GLib.MainContext ? gcontext = null);

        [CCode (cname = "g_zmq_source_router_new")]
        public ZmqSource.Router (size_t max_size = 0, GLib.MainContext ? gcontext = null);

        // take the ownership of the zmq socket
        [CCode (cname = "g_zmq_source_new_from_socket")]
        public ZmqSource.FromSocket (void* zmq_socket, size_t max_size = 0, GLib.MainContext ? gcontext = null);

        public EasyZmq.SocketType socket_type {
            [CCode (cname = "g_easyzmq_get_socket_type")] get;
        }

        // default false: send never blocks the main loop
        public bool blocking_send {
            [CCode (cname = "g_easyzmq_get_blocking_send")] get;
            [CCode (cname = "g_easyzmq_set_blocking_send")] set;
        }

        [CCode (cname = "GZmqMonitorFunc", has_target = false)]
        public delegate void monitor_callback (EasyZmq.ZmqSource zmq_source, EasyZmq.monitor_event event, uint32 value, uint8[] address);

        [CCode (cname = "get_zmq_socket_monitor")]
        public EasyZmq.ZmqSource Monitor (monitor_callback ? cb = null);

        [CCode (cname = "g_easyzmq_bind")]
        public bool Bind (string addr) throws EasyZmq.Error;

        [CCode (cname = "g_easyzmq_unbind")]
        public bool Unbind (string addr) throws EasyZmq.Error;

        [CCode (cname = "g_easyzmq_connect")]
        public bool Connect (string addr) throws EasyZmq.Error;

        [CCode (cname = "g_easyzmq_disconnect")]
        public bool Disconnect (string addr) throws EasyZmq.Error;

        [CCode (cname = "GZmqSourceFunc", has_target = false)]
        public delegate bool callback(EasyZmq.ZmqSource zmq_source, uint8[] buf, bool more, void* user_data);

        // Request

        [CCode (cname = "send_zmq_req")]
        public bool SendReq(uint timeoutms, uint8[] buf, callback cb, void* user_data = null, GLib.DestroyNotify ? udestroy = null);
        
        [CCode (cname = "send_zmq_req_take")]
        public bool SendReqTake(uint timeoutms, owned uint8[] buf, GLib.DestroyNotify ? ddestroy = GLib.free, callback ? cb = null, void* user_data = null, GLib.DestroyNotify ? udestroy = null);

        // GTask based async API: errors GLib.IOError.TIMED_OUT, GLib.IOError.CANCELLED, EasyZmq.Error
        // multipart answer: Request returns the frames concatenated, RequestMultipart one Bytes for each frame

        [CCode (cname = "g_easyzmq_request_async", finish_name = "g_easyzmq_request_finish")]
        public async GLib.Bytes Request (GLib.Bytes request, uint timeoutms = 0, GLib.Cancellable ? cancellable = null) throws GLib.Error;

        [CCode (cname = "g_easyzmq_request_async", finish_name = "g_easyzmq_request_multipart_finish")]
        public async GLib.GenericArray<GLib.Bytes> RequestMultipart (GLib.Bytes request, uint timeoutms = 0, GLib.Cancellable ? cancellable = null) throws GLib.Error;

        // Sub, Pair, Rep: wait the next message (when nobody waits the source is paused)

        [CCode (cname = "g_easyzmq_receive_async", finish_name = "g_easyzmq_receive_finish")]
        public async GLib.Bytes Receive (GLib.Cancellable ? cancellable = null) throws GLib.Error;

        [CCode (cname = "g_easyzmq_receive_async", finish_name = "g_easyzmq_receive_multipart_finish")]
        public async GLib.GenericArray<GLib.Bytes> ReceiveMultipart (GLib.Cancellable ? cancellable = null) throws GLib.Error;

        // GBytes API (zero copy)

        [CCode (cname = "GZmqSourceBytesFunc")]
        public delegate bool BytesCallback (EasyZmq.ZmqSource zmq_source, GLib.Bytes ? data, bool more);

        // Sub, Pair, Rep: data == null means receive error
        [CCode (cname = "g_easyzmq_recv_bytes")]
        public bool RecvBytes (owned BytesCallback cb);

        // Pub, Pair, Rep
        [CCode (cname = "g_easyzmq_send_bytes")]
        public bool SendBytes (GLib.Bytes data);

        // frame under receive, valid only inside a receive callback (zero copy without pre-allocated memory)
        [CCode (cname = "g_easyzmq_get_current_bytes")]
        public GLib.Bytes ? GetCurrentBytes ();

        public bool paused {
            [CCode (cname = "g_easyzmq_get_paused")] get;
            [CCode (cname = "g_easyzmq_set_paused")] set;
        }

        // old async API: bufout is valid only until the next yield
        [Version (deprecated = true, replacement = "Request")]
        [CCode (cname = "send_zmq_req_async", finish_name = "send_zmq_req_finish")]
        public async void SendReq_async(uint timeoutms, uint8[] bufin, out unowned uint8[] bufout, out bool more);

        [Version (deprecated = true, replacement = "Request")]
        [CCode (cname = "send_zmq_req_take_async", finish_name = "send_zmq_req_finish")]
        public async void SendReqTake_async(uint timeoutms, owned uint8[] bufin, out unowned uint8[] bufout, out bool more);

        // Replay

        [CCode (cname = "recv_zmq_rep")]
        public bool RecvRep(callback cb, void* user_data = null, GLib.DestroyNotify ? unotify = null);

        [CCode (cname = "send_zmq_rep")]
        public bool SendRep(uint8[] buf);
        
        [CCode (cname = "send_zmq_rep_take")]
        public bool SendRepTake(owned uint8[] buf, GLib.DestroyNotify ? ddestroy = GLib.free);

        // Publisher

        [CCode (cname = "send_zmq_pub")]
        public bool SendPub(uint8[] buf);
        
        [CCode (cname = "send_zmq_pub_take")]
        public bool SendPubTake(owned uint8[] buf, GLib.DestroyNotify ? ddestroy = GLib.free);

        // Subscriber

        [CCode (cname = "recv_zmq_sub")]
        public bool RecvSub(callback cb, void* user_data = null, GLib.DestroyNotify ? unotify = null);

        [CCode (cname = "subscribe_zmq_sub")]
        public bool SubscribeSub(uint8[] filter) throws EasyZmq.Error;

        [CCode (cname = "unsubscribe_zmq_sub")]
        public bool UnsubscribeSub(uint8[] filter) throws EasyZmq.Error;

        // Pair

        [CCode (cname = "recv_zmq_pair")]
        public bool RecvPair(callback cb, void* user_data = null, GLib.DestroyNotify ? unotify = null);

        [CCode (cname = "send_zmq_pair")]
        public bool SendPair(uint8[] buf);

        [CCode (cname = "send_zmq_pair_take")]
        public bool SendPairTake(owned uint8[] buf, GLib.DestroyNotify ? ddestroy = GLib.free);

    }
}