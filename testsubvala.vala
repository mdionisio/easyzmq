MainLoop ? loop = null;

bool callback(EasyZmq.ZmqSource zmq_source, uint8[] buf, bool more, void* user_data) {
	//stdout.printf("recv: %d more: %d\n", buf.length, (int)more);
	stdout.printf("recv: %.*s more: %d\n", buf.length, (string)buf, (int)more);
	
	return true;
}

void main() {
	loop = new MainLoop();
	EasyZmq.Init();

	try {
		var req = new EasyZmq.ZmqSource.Sub();
		req.Connect("tcp://127.0.0.1:61000");
		req.RecvSub(callback);
		req.SubscribeSub("".data);
		
		loop.run();
	} catch (EasyZmq.Error er) {
		stdout.printf("error connect\n");
	}
	
	EasyZmq.Deinit();
}