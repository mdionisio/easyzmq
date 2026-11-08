// async REQ client: run testzmqrepasyncvala (or testzmqresp) and then this example
// usage: testzmqreqasyncvala [address]   (default tcp://127.0.0.1:5555)

string str (Bytes b) {
    return "%.*s".printf ((int) b.length, (string) b.get_data ());
}

async void run_client (string address) {
    var req = new EasyZmq.ZmqSource.Req ();
    try {
        req.Connect (address);
    } catch (EasyZmq.Error e) {
        stderr.printf ("connect error: %s\n", e.message);
        return;
    }

    for (int i = 0; i < 5; i++) {
        try {
            // with a timeout the socket can send a new request also if the answer is lost
            Bytes answer = yield req.Request (new Bytes ("request %d".printf (i).data), 2000);
            print ("answer: '%s'\n", str (answer));
        } catch (IOError.TIMED_OUT e) {
            print ("request %d: no answer in 2 s (is the server running?)\n", i);
        } catch (EasyZmq.Error e) {
            // e.code is the zmq errno
            print ("request %d: send error %d %s\n", i, e.code, e.message);
        } catch (Error e) {
            print ("request %d: %s\n", i, e.message);
        }
    }

}

int main (string[] args) {
    string address = (args.length > 1) ? args[1] : "tcp://127.0.0.1:5555";
    var loop = new MainLoop ();
    EasyZmq.Init ();

    run_client.begin (address, (obj, res) => {
        run_client.end (res);
        loop.quit ();
    });
    loop.run ();

    EasyZmq.Deinit ();
    return 0;
}
