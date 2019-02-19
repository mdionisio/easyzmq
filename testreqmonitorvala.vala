MainLoop ? loop = null;

bool callback (EasyZmq.ZmqSource zmq_source, uint8[] buf, bool more, void * user_data) {
    if (buf.length == 0) {
        stdout.printf ("error\n");
    } else {
        stdout.printf ("recv: %.*s more: %d\n", buf.length, (string) buf, (int) more);
    }

    GLib.Timeout.add_seconds (2, () => {
        stdout.printf ("disconnecting...\n");
        try {
            bool retval = zmq_source.Disconnect ("tcp://127.0.0.1:5555");
            stdout.printf ("disconnecting: %d\n", (int) retval);
        } catch (EasyZmq.Error er) {
            stdout.printf ("error disconnect: %s\n", er.message);
        }
        return false;
    });

    GLib.Timeout.add_seconds (5, () => {
        stdout.printf ("closing...\n");
        loop.quit ();
        return false;
    });

    return true;
}

void monitor_callback (EasyZmq.ZmqSource zmq_source, EasyZmq.monitor_event event, uint32 value, uint8[] buf) {
    stdout.printf ("event: %u value: %u address: %.*s\n", (uint) event, (uint) value, buf.length, (string) buf);
}

void main () {
    loop = new MainLoop ();
    EasyZmq.Init ();

    try {
        var req = new EasyZmq.ZmqSource.Req ();
        req.Monitor (monitor_callback);
        req.Connect ("tcp://127.0.0.1:5555");
        req.SendReq (20000, "pippo".data, callback);

        loop.run ();
        stdout.printf ("closed\n");
    } catch (EasyZmq.Error er) {
        stdout.printf ("error connect\n");
    }
    loop = null;
    stdout.printf ("all destroy\n");

    EasyZmq.Deinit ();
}