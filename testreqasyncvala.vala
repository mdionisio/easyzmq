
async void mainasync() {
	try {
		var req1 = new EasyZmq.ZmqSource.Req();
		req1.Connect("tcp://127.0.0.1:5555");

		var req2 = new EasyZmq.ZmqSource.Req();
		req2.Connect("tcp://127.0.0.1:5556");

		unowned uint8[] bufout = null;

		yield req1.SendReq_async(10000, "pippo req1".data, out bufout);

		stdout.printf("req1: %d\n", bufout.length);

		bufout = null;

		yield req2.SendReq_async(10000, "pippo req2".data, out bufout);

		stdout.printf("req2: %d\n", bufout.length);
	} catch (EasyZmq.Error er) {
		stdout.printf("error connect\n");
	}
}

void main() {
	MainLoop loop = new MainLoop();
	EasyZmq.Init();

	mainasync.begin((obj, res) => {
		mainasync.end(res);
		print("\nEND\n");
		loop.quit();
	});

	loop.run();
	
	EasyZmq.Deinit();
}