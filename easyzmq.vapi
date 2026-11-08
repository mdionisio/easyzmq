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
        MONITOR_STOPPED,
        HANDSHAKE_FAILED_NO_DETAIL,
        HANDSHAKE_SUCCEEDED,
        HANDSHAKE_FAILED_PROTOCOL,
        HANDSHAKE_FAILED_AUTH;
    }

    [CCode (cname = "easyzmq_socket_t", cprefix = "EASY_ZMQ_", has_type_id = false)]
    public enum SocketType {
        PUB,
        SUB,
        REQ,
        REP,
        PAIR,
        ROUTER,
        UNKNOWN,
        DEALER,
        PUSH,
        PULL,
        XPUB,
        XSUB
    }

    // most used zmq socket options (any ZMQ_* value can be used casting an int)
    [CCode (cname = "int", cprefix = "ZMQ_", has_type_id = false, cheader_filename = "zmq.h")]
    public enum Option {
        AFFINITY,
        ROUTING_ID,
        IDENTITY,
        LINGER,
        RECONNECT_IVL,
        RECONNECT_IVL_MAX,
        BACKLOG,
        MAXMSGSIZE,
        SNDHWM,
        RCVHWM,
        SNDBUF,
        RCVBUF,
        SNDTIMEO,
        RCVTIMEO,
        IPV6,
        IMMEDIATE,
        CONFLATE,
        TCP_KEEPALIVE,
        TCP_KEEPALIVE_IDLE,
        TCP_KEEPALIVE_CNT,
        TCP_KEEPALIVE_INTVL,
        ROUTER_MANDATORY,
        XPUB_VERBOSE,
        PLAIN_SERVER,
        PLAIN_USERNAME,
        PLAIN_PASSWORD,
        CURVE_SERVER,
        CURVE_PUBLICKEY,
        CURVE_SECRETKEY,
        CURVE_SERVERKEY,
        ZAP_DOMAIN,
        HEARTBEAT_IVL,
        HEARTBEAT_TIMEOUT,
        HEARTBEAT_TTL,
        LAST_ENDPOINT,
        TYPE
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

        [CCode (cname = "g_zmq_source_dealer_new")]
        public ZmqSource.Dealer (size_t max_size = 0, GLib.MainContext ? gcontext = null);

        [CCode (cname = "g_zmq_source_push_new")]
        public ZmqSource.Push (GLib.MainContext ? gcontext = null);

        [CCode (cname = "g_zmq_source_pull_new")]
        public ZmqSource.Pull (size_t max_size = 0, GLib.MainContext ? gcontext = null);

        [CCode (cname = "g_zmq_source_xpub_new")]
        public ZmqSource.XPub (size_t max_size = 0, GLib.MainContext ? gcontext = null);

        [CCode (cname = "g_zmq_source_xsub_new")]
        public ZmqSource.XSub (size_t max_size = 0, GLib.MainContext ? gcontext = null);

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

        [CCode (cname = "g_easyzmq_set_option_int")]
        public bool SetOptionInt (EasyZmq.Option option, int value) throws EasyZmq.Error;

        [CCode (cname = "g_easyzmq_get_option_int")]
        public bool GetOptionInt (EasyZmq.Option option, out int value) throws EasyZmq.Error;

        [CCode (cname = "g_easyzmq_set_option_int64")]
        public bool SetOptionInt64 (EasyZmq.Option option, int64 value) throws EasyZmq.Error;

        [CCode (cname = "g_easyzmq_get_option_int64")]
        public bool GetOptionInt64 (EasyZmq.Option option, out int64 value) throws EasyZmq.Error;

        [CCode (cname = "g_easyzmq_set_option_bytes")]
        public bool SetOptionBytes (EasyZmq.Option option, uint8[] value) throws EasyZmq.Error;

        [CCode (cname = "g_easyzmq_set_option_string")]
        public bool SetOptionString (EasyZmq.Option option, string value) throws EasyZmq.Error;

        [CCode (cname = "g_easyzmq_get_option_string")]
        public string GetOptionString (EasyZmq.Option option) throws EasyZmq.Error;

        [CCode (cname = "g_easyzmq_get_socket")]
        public void* GetSocket ();

        [CCode (cname = "GZmqMonitorFunc", has_target = false)]
        public delegate void monitor_callback (EasyZmq.ZmqSource zmq_source, EasyZmq.monitor_event event, uint32 value, uint8[] address);

        [CCode (cname = "get_zmq_socket_monitor")]
        public EasyZmq.ZmqSource Monitor (monitor_callback ? cb = null);

        // all events, closure allowed
        [CCode (cname = "GZmqMonitorFullFunc")]
        public delegate void MonitorCallback (EasyZmq.ZmqSource monitor, EasyZmq.monitor_event event, uint32 value, string address);

        [CCode (cname = "get_zmq_socket_monitor_full")]
        public EasyZmq.ZmqSource MonitorFull (owned MonitorCallback cb);

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

        // Generic receive (Sub, Pair, Rep, Dealer, Pull, XPub, XSub) and send (Pub, Pair, Rep, Dealer, Push, XPub, XSub)

        [CCode (cname = "g_easyzmq_recv")]
        public bool Recv (callback cb, void* user_data = null, GLib.DestroyNotify ? unotify = null);

        [CCode (cname = "g_easyzmq_send")]
        public bool Send (uint8[] buf);

        [CCode (cname = "g_easyzmq_send_take")]
        public bool SendTake (owned uint8[] buf, GLib.DestroyNotify ? ddestroy = GLib.free);

        // multipart: frames sent with SendMore, the last one with any normal send
        [CCode (cname = "g_easyzmq_send_more")]
        public bool SendMore (uint8[] buf);

        [CCode (cname = "g_easyzmq_send_bytes_more")]
        public bool SendBytesMore (GLib.Bytes data);

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