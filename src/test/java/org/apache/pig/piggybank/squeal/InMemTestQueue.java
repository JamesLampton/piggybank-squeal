package org.apache.pig.piggybank.squeal;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class InMemTestQueue {
	static ConcurrentHashMap<String, BlockingQueue<byte[]>> _queues = new ConcurrentHashMap<String, BlockingQueue<byte[]>>();
	
	static synchronized public BlockingQueue<byte[]> getQueue(String name) {
		BlockingQueue<byte[]> cur = _queues.get(name);
		if (cur == null) {
			cur = new ArrayBlockingQueue<byte[]>(1024);
			_queues.put(name, cur);
		}
		
		return cur;
	}
}
