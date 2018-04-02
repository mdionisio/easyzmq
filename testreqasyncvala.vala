
void main() {
	MainLoop loop = new MainLoop();
	EasyZmq.Init();

	try {
		var req = new EasyZmq.ZmqSource.Req();
		req.Connect("tcp://127.0.0.1:5555");
		req.SendReq_async.begin(20000, "pippo".data, (obj, res) => {
			uint8[] bufout = {};
			req.SendReq_async.end(res, out bufout);
			print("recev: %d\n", bufout.length);
			loop.quit();
		});
		
		loop.run();
	} catch (EasyZmq.Error er) {
		stdout.printf("error connect\n");
	}
	
	EasyZmq.Deinit();
}