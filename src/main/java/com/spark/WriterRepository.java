package com.spark;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class WriterRepository {
	private static BlockingQueue<WriterInfo> queue = new LinkedBlockingQueue<>(100000);
	private static CountUpAndDownLatch countUpAndDownLatch = new CountUpAndDownLatch(0);
	
	public static CountUpAndDownLatch getCountUpAndDownLatch() {
		return countUpAndDownLatch;
	}

	public static BlockingQueue<WriterInfo> getQueue() {
		return queue;
	}

}
