MainLoop ? loop = null;

bool callback(EasyZmq.ZmqSource zmq_source, uint8[] buf, bool more, void* user_data) {
	stdout.printf("recv: %.*s more: %d\n", buf.length, (string)buf, (int)more);

	zmq_source.SendRep("pippo".data);

	return true;
}

void main() {
	loop = new MainLoop();
	EasyZmq.Init();

	try {
		var req = new EasyZmq.ZmqSource.Rep();
		req.Bind("tcp://0.0.0.0:5555");
		req.RecvRep(callback);
		
		loop.run();
	} catch (EasyZmq.Error er) {
		stdout.printf("error connect\n");
	}
	
	EasyZmq.Deinit();
}