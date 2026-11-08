// async subscriber: run testzmqpub and then this example
// usage: testzmqsubasyncvala [address] [filter]   (default tcp://127.0.0.1:5555, no filter)

string str (Bytes b) {
    return "%.*s".printf ((int) b.length, (string) b.get_data ());
}

async void receive_loop (EasyZmq.ZmqSource sub, Cancellable cancellable) {
    while (true) {
        try {
            // multipart messages: one Bytes for each frame
            GenericArray<Bytes> frames = yield sub.ReceiveMultipart (cancellable);
            var line = new StringBuilder ();
            frames.foreach ((frame) => {
                line.append_printf ("[%s]", str (frame));
            });
            print ("%s\n", line.str);
        } catch (IOError.CANCELLED e) {
            return;
        } catch (Error e) {
            stderr.printf ("error: %s\n", e.message);
        }
    }
}

int main (string[] args) {
    string address = (args.length > 1) ? args[1] : "tcp://127.0.0.1:5555";
    string filter = (args.length > 2) ? args[2] : "";
    var loop = new MainLoop ();
    var stop = new Cancellable ();
    EasyZmq.Init ();

    Unix.signal_add (ProcessSignal.INT, () => {
        stop.cancel ();
        loop.quit ();
        return Source.REMOVE;
    });

    try {
        var sub = new EasyZmq.ZmqSource.Sub ();
        sub.Connect (address);
        sub.SubscribeSub (filter.data);
        receive_loop.begin (sub, stop);
        loop.run ();
    } catch (EasyZmq.Error e) {
        stderr.printf ("connect error: %s\n", e.message);
    }

    EasyZmq.Deinit ();
    return 0;
}
