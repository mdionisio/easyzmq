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

    [CCode (cname = "easy_zmq_init")]
    public void Init();

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

        [CCode (cname = "send_zmq_req_async", finish_name = "send_zmq_req_finish")]
        public async void SendReq_async(uint timeoutms, uint8[] bufin, out unowned uint8[] bufout, out bool more);

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