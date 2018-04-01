[CCode (cheader_filename = "easyzmq.h")]
namespace EasyZmq {
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
        ENODEV;
		public static GLib.Quark quark ();
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

        [CCode (cname = "g_easyzmq_bind")]
        public bool Bind (string addr) throws EasyZmq.Error;

        [CCode (cname = "g_easyzmq_connect")]
        public bool Connect (string addr) throws EasyZmq.Error;

        [CCode (cname = "GZmqSourceFunc", has_target = false)]
        public delegate bool callback(EasyZmq.ZmqSource zmq_source, uint8 buf[], void* user_data);

        [CCode (cname = "send_zmq_req")]
        public bool SendReq(uint timeoutms, uint8 buf[], callback cb, void* user_data = null, GLib.DestroyNotify ? udestroy = null);
        
        [CCode (cname = "send_zmq_req_take")]
        public bool SendReqTake(uint timeoutms, owned uint8 buf[], GLib.DestroyNotify ? ddestroy = null, callback ? cb = null, void* user_data = null, GLib.DestroyNotify ? udestroy = null);

    }
}