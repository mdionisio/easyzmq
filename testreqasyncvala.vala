
async void mainasync () {
    try {
        var req1 = new EasyZmq.ZmqSource.Req ();
        req1.Connect ("tcp://127.0.0.1:5555");

        var req2 = new EasyZmq.ZmqSource.Req ();
        req2.Connect ("tcp://127.0.0.1:5556");

        unowned uint8[] bufout = null;
        bool more = false;

        yield req1.SendReq_async (10000, "pippo req1".data, out bufout, out more);

        stdout.printf ("req1: '%.*s' more: %d\n", bufout.length, (string) bufout, (int) more);

        bufout = null;
        uint8[] bufout_owned = null;

        yield req2.SendReqTake_async (10000, "pippo req2".data, out bufout_owned, out more);

        stdout.printf ("req2: '%.*s' more: %d\n", bufout_owned.length, (string) bufout_owned, (int) more);
    } catch (EasyZmq.Error er) {
        stdout.printf ("error connect\n");
    }
}

void main () {
    MainLoop loop = new MainLoop ();
    EasyZmq.Init ();

    mainasync.begin ((obj, res) => {
        mainasync.end (res);
        print ("\nEND\n");
        loop.quit ();
    });

    loop.run ();

    EasyZmq.Deinit ();
}