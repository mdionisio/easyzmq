// async REP server: run it and then testzmqreqasyncvala (or testzmqreq)
// usage: testzmqrepasyncvala [address]   (default tcp://0.0.0.0:5555)

string str (Bytes b) {
    return "%.*s".printf ((int) b.length, (string) b.get_data ());
}

async void serve (EasyZmq.ZmqSource rep, Cancellable cancellable) {
    int counter = 0;
    while (true) {
        try {
            // the source is paused while the request is processed:
            // the next request stays queued in zmq
            Bytes request = yield rep.Receive (cancellable);
            print ("request: '%s'\n", str (request));

            // simulate a slow answer without blocking the main loop
            Timeout.add (100, serve.callback);
            yield;

            rep.SendRep ("answer %d to '%s'".printf (counter++, str (request)).data);
        } catch (IOError.CANCELLED e) {
            print ("stop\n");
            return;
        } catch (Error e) {
            stderr.printf ("error: %s\n", e.message);
        }
    }
}

int main (string[] args) {
    string address = (args.length > 1) ? args[1] : "tcp://0.0.0.0:5555";
    var loop = new MainLoop ();
    var stop = new Cancellable ();
    EasyZmq.Init ();

    Unix.signal_add (ProcessSignal.INT, () => {
        stop.cancel ();
        loop.quit ();
        return Source.REMOVE;
    });

    try {
        var rep = new EasyZmq.ZmqSource.Rep ();
        rep.Bind (address);
        print ("listen on %s (ctrl-c to stop)\n", address);
        serve.begin (rep, stop);
        loop.run ();
    } catch (EasyZmq.Error e) {
        stderr.printf ("bind error: %s\n", e.message);
    }

    EasyZmq.Deinit ();
    return 0;
}
