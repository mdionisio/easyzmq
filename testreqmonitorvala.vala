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


void main () {
    loop = new MainLoop ();
    EasyZmq.Init ();

    try {
        var req = new EasyZmq.ZmqSource.Req ();
        // all the events (also connect retried, closed and handshake), closure allowed
        int retries = 0;
        var monitor = req.MonitorFull ((m, event, value, address) => {
            if (event == EasyZmq.monitor_event.CONNECT_RETRIED) {
                retries++;
            }
            stdout.printf ("event: %s value: %u address: %s (retries: %d)\n", event.to_string (), (uint) value, address, retries);
        });
        req.Connect ("tcp://127.0.0.1:5555");
        req.SendReq (20000, "pippo".data, callback);

        loop.run ();
        monitor = null;
        stdout.printf ("closed\n");
    } catch (EasyZmq.Error er) {
        stdout.printf ("error connect\n");
    }
    loop = null;
    stdout.printf ("all destroy\n");

    EasyZmq.Deinit ();
}