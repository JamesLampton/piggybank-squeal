/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
