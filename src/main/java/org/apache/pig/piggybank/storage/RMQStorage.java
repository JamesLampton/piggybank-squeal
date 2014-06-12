package org.apache.pig.piggybank.storage;

import java.io.IOException;

import org.apache.pig.builtin.PigStorage;
import org.apache.pig.builtin.PigStreaming;
import org.apache.pig.data.Tuple;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class RMQStorage extends PigStorage {

	private String rabbitURI;
	boolean connected = false;
	private ConnectionFactory factory;
	private Connection connection;
	private Channel channel;
	private String exchangeName;
	private PigStreaming ps = new PigStreaming();

	public RMQStorage(String rabbitURI, String exchangeName) {
		this.rabbitURI = rabbitURI;
		this.exchangeName = exchangeName;
	}
	
	public void connect() {
		try {
			factory = new ConnectionFactory();
			factory.setUri(rabbitURI);

			connection = factory.newConnection();
			channel = connection.createChannel();

			channel.exchangeDeclare(exchangeName, "fanout", true);
			connected = true;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to setup reader", e);
		}
	}
	
	@Override
	public void putNext(Tuple t) throws IOException {
		if (connected == false) {
			connect();
		}
		
		byte[] buf = ps.serialize(t);
		channel.basicPublish(exchangeName, "", null, buf);
	}
}
