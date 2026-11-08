// self contained example (inproc, no network) of the async API:
// REP server and SUB receiver written as async loops, REQ client with timeout and cancellable.
// It is also run by "meson test".

const string REP_ADDRESS = "inproc://example-rep";
const string PUB_ADDRESS = "inproc://example-pub";

// zmq messages are not NUL terminated
string str (Bytes b) {
    return "%.*s".printf ((int) b.length, (string) b.get_data ());
}

// REP server: one request at a time, answer with the uppercase request
async void rep_server (EasyZmq.ZmqSource rep, Cancellable cancellable) {
    while (!cancellable.is_cancelled ()) {
        try {
            Bytes request = yield rep.Receive (cancellable);
            string text = str (request);
            rep.SendRep (text.up ().data);
        } catch (IOError.CANCELLED e) {
            break;
        } catch (Error e) {
            stderr.printf ("server error: %s\n", e.message);
        }
    }
    print ("server stopped\n");
}

// SUB receiver: print the received messages until cancelled
async void subscriber (EasyZmq.ZmqSource sub, Cancellable cancellable, int expected) {
    for (int i = 0; i < expected; i++) {
        try {
            Bytes msg = yield sub.Receive (cancellable);
            print ("sub received: '%s'\n", str (msg));
        } catch (Error e) {
            print ("sub stopped: %s\n", e.message);
            return;
        }
    }
}

async int client () throws Error {
    int errors = 0;

    var req = new EasyZmq.ZmqSource.Req ();
    req.Connect (REP_ADDRESS);

    // 1) simple requests: the answer is a GLib.Bytes owned by the caller (no lifetime problems)
    for (int i = 0; i < 3; i++) {
        var request = new Bytes ("hello %d".printf (i).data);
        Bytes answer = yield req.Request (request, 1000);
        print ("answer: '%s'\n", str (answer));
        if (str (answer) != "HELLO %d".printf (i)) {
            errors++;
        }
    }

    // 2) more requests in parallel: one REQ socket for each request
    var req_a = new EasyZmq.ZmqSource.Req ();
    var req_b = new EasyZmq.ZmqSource.Req ();
    req_a.Connect (REP_ADDRESS);
    req_b.Connect (REP_ADDRESS);
    Bytes ? answer_a = null;
    Bytes ? answer_b = null;
    int pending = 2;
    req_a.Request.begin (new Bytes ("a".data), 1000, null, (obj, res) => {
        try {
            answer_a = req_a.Request.end (res);
        } catch (Error e) {
            stderr.printf ("req_a: %s\n", e.message);
        }
        if (--pending == 0) {
            client.callback ();
        }
    });
    req_b.Request.begin (new Bytes ("b".data), 1000, null, (obj, res) => {
        try {
            answer_b = req_b.Request.end (res);
        } catch (Error e) {
            stderr.printf ("req_b: %s\n", e.message);
        }
        if (--pending == 0) {
            client.callback ();
        }
    });
    yield;
    print ("parallel answers: '%s' '%s'\n", str (answer_a), str (answer_b));
    if ((str (answer_a) != "A") || (str (answer_b) != "B")) {
        errors++;
    }

    // 3) timeout: nobody is bound on this address
    var lonely = new EasyZmq.ZmqSource.Req ();
    lonely.Connect ("inproc://nobody");
    try {
        yield lonely.Request (new Bytes ("anybody?".data), 200);
        errors++;
    } catch (IOError.TIMED_OUT e) {
        print ("timeout as expected: %s\n", e.message);
    }

    // 4) cancel: the request is cancelled after 100 ms
    var cancellable = new Cancellable ();
    Timeout.add (100, () => {
        cancellable.cancel ();
        return Source.REMOVE;
    });
    try {
        yield lonely.Request (new Bytes ("anybody?".data), 5000, cancellable);
        errors++;
    } catch (IOError.CANCELLED e) {
        print ("cancelled as expected\n");
    }

    // 5) GBytes callback API (lambda with captured variables)
    var pair_a = new EasyZmq.ZmqSource.Pair ();
    var pair_b = new EasyZmq.ZmqSource.Pair ();
    pair_a.Bind ("inproc://example-pair");
    pair_b.Connect ("inproc://example-pair");
    var received = new GenericArray<Bytes> ();
    pair_a.RecvBytes ((source, data, more) => {
        received.add (data);
        if (received.length == 2) {
            client.callback ();
        }
        return true;
    });
    pair_b.SendBytes (new Bytes ("first".data));
    pair_b.SendBytes (new Bytes ("second".data));
    yield;
    print ("pair received: '%s' '%s'\n", str (received[0]), str (received[1]));
    if (str (received[1]) != "second") {
        errors++;
    }

    return errors;
}

int main () {
    var loop = new MainLoop ();
    EasyZmq.Init ();
    int errors = 0;

    try {
        var rep = new EasyZmq.ZmqSource.Rep ();
        rep.Bind (REP_ADDRESS);

        var pub = new EasyZmq.ZmqSource.Pub ();
        pub.Bind (PUB_ADDRESS);
        var sub = new EasyZmq.ZmqSource.Sub ();
        sub.Connect (PUB_ADDRESS);
        sub.SubscribeSub ("".data);

        var stop = new Cancellable ();
        rep_server.begin (rep, stop);
        subscriber.begin (sub, stop, 3);

        // publish after the subscription is arrived
        Timeout.add (50, () => {
            for (int i = 0; i < 3; i++) {
                pub.SendPub ("event %d".printf (i).data);
            }
            return Source.REMOVE;
        });

        client.begin ((obj, res) => {
            try {
                errors = client.end (res);
            } catch (Error e) {
                stderr.printf ("client error: %s\n", e.message);
                errors++;
            }
            stop.cancel ();
            // let the async loops see the cancel
            Idle.add (() => {
                loop.quit ();
                return Source.REMOVE;
            });
        });

        loop.run ();
    } catch (EasyZmq.Error e) {
        stderr.printf ("error: %s\n", e.message);
        errors++;
    }

    EasyZmq.Deinit ();
    print ("END errors: %d\n", errors);
    return (errors == 0) ? 0 : 1;
}
