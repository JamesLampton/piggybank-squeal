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

package org.apache.pig.piggybank.squeal.metrics;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.pig.impl.PigContext;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.jboss.netty.util.CharsetUtil;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

public class RMQMetricsTransport extends AbstractMetricsTransport {

	static public final String RMQ_URI_KEY = "pig.streaming.metrics.transport.rmq.uri";
	static public final String RMQ_EXCHANGE_KEY = "pig.streaming.metrics.transport.rmq.exchange";

	private String rabbitURI;
	private String exchangeName;
	private ConnectionFactory factory;
	private Connection connection;
	private com.rabbitmq.client.Channel channel;
	
	public void initialize(PigContext pc) {
		initialize(pc.getProperties());
	}
	
	@Override
	public void initialize(Map props) {
		super.initialize(props);
		
		try {
			rabbitURI = (String) props.get(RMQ_URI_KEY);
			factory = new ConnectionFactory();
			factory.setUri(rabbitURI);

			connection = factory.newConnection();
			channel = connection.createChannel();

			exchangeName = (String) props.get(RMQ_EXCHANGE_KEY);
			channel.exchangeDeclare(exchangeName, "fanout", true);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to setup writer", e);
		}
	}

	@Override
	public void send(byte[] buf) {
		try {
			channel.basicPublish(exchangeName, "", null, buf);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void runListener(String queueName, final OutputStream os) throws Exception {
		if (queueName != null) {
			channel.queueDeclare(queueName, true, false, false, null);
		} else {
			queueName = channel.queueDeclare().getQueue();				
		}
		
		channel.queueBind(queueName, exchangeName, "");

		QueueingConsumer consumer = new QueueingConsumer(channel);
		channel.basicConsume(queueName, false, consumer);
		int c = 0;
		
		while (true) {
			Delivery d = consumer.nextDelivery(5000);
			c += 1;
			if (d != null) {
				os.write(d.getBody());
				channel.basicAck(d.getEnvelope().getDeliveryTag(), true);
			}
			if (d == null || c > 1000) {
				os.flush();
				c = 0;
			}
		}
	}
	
	public static void main(String args[]) throws Exception {
		boolean runServer = true;
		int options_start = 0;
		
		String uri = null;
		String exchange = null;
		String queue = null;
		String out = null;

		// Parse the arguments.
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-runListener")) {
				runServer = true;
			} else if (args[i].equals("-transmit")) {
				runServer = false;
			} else if (args[i].equals("-rabbitURI")) {
				uri = args[i+1];
				i++;
			} else if (args[i].equals("-exchange")) {
				exchange = args[i+1];
				i++;
			} else if (args[i].equals("-queue")) {
				queue = args[i+1];
				i++;
			} else if (args[i].equals("-out")) {
				out = args[i+1];
				i++;
			} else {
				System.err.println("Unknown option: " + args[i]);
				return;
			}
		}
		
		// Check for the necessary elements.
		Properties p = new Properties();
		
		if (uri == null) {
			System.err.println("option: -rabbitURI required");
			return;
		}
		p.setProperty(RMQ_URI_KEY, uri);
		
		if (exchange== null) {
			System.err.println("option: -exchange required");
			return;
		}
		p.setProperty(RMQ_EXCHANGE_KEY, exchange);
		p.setProperty(MetricsTransportFactory.METRICS_TRANSPORT_KEY, RMQMetricsTransport.class.getName());

		System.err.println("Configuring " + uri + " exchange: " + exchange);
		RMQMetricsTransport mt = (RMQMetricsTransport) MetricsTransportFactory.getInstance(p, ClassLoader.getSystemClassLoader());
				
		if (runServer) {
			final OutputStream os;
			if (out != null) {
				os = new FileOutputStream(out);
			} else {
				os = System.out;
				out = "<stdout>";
			}

			System.err.println("Starting listener writing to: " + out);
		
			mt.runListener(queue, os);;
		} else {
			System.err.println("Sending...");
			
			BufferedReader bi = new BufferedReader(new InputStreamReader(System.in));
			String line;
			while ((line = bi.readLine()) != null) {
				System.out.println("SENDING: " + line);
				mt.send(line.getBytes());
			}
			
		}
	}
}
