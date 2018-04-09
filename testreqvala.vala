MainLoop ? loop = null;

bool callback(EasyZmq.ZmqSource zmq_source, uint8[] buf, bool more, void* user_data) {
	if (buf.length == 0) {
		stdout.printf("error\n");
	} else {
		stdout.printf("recv: %.*s more: %d\n", buf.length, (string)buf, (int)more);
	}
	loop.quit();
	return true;
}

void main() {
	loop = new MainLoop();
	EasyZmq.Init();

	try {
		var req = new EasyZmq.ZmqSource.Req();
		req.Connect("tcp://127.0.0.1:5555");
		req.SendReq(20000, "pippo".data, callback);
		
		loop.run();
	} catch (EasyZmq.Error er) {
		stdout.printf("error connect\n");
	}
	
	EasyZmq.Deinit();
}