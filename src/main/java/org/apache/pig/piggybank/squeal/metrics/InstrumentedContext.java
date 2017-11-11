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

//import java.lang.reflect.Field;
//import java.net.InetAddress;
//import java.net.InetSocketAddress;
//import java.net.UnknownHostException;
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicLong;
//
//import org.jboss.netty.bootstrap.ClientBootstrap;
//import org.jboss.netty.channel.Channel;
//import org.jboss.netty.channel.ChannelFactory;
//import org.jboss.netty.channel.ChannelFuture;
//import org.jboss.netty.channel.ChannelPipeline;
//import org.jboss.netty.channel.ChannelPipelineFactory;
//import org.jboss.netty.channel.Channels;
//import org.jboss.netty.channel.SimpleChannelHandler;
//import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
//import org.jboss.netty.handler.codec.string.StringEncoder;
//import org.jboss.netty.util.CharsetUtil;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

//import backtype.storm.messaging.IConnection;
//import backtype.storm.messaging.TaskMessage;
//import backtype.storm.messaging.netty.Client;
//import backtype.storm.messaging.netty.Context;

// FIXME: Rethink or remove.
public class InstrumentedContext {

//public class InstrumentedContext extends Context {

//	class InstrumentedClientProxy implements IConnection {
//
//		private Client wrapped;
//		private AtomicLong pendings;
//		private AtomicLong messages_sent;
//		private AtomicLong bytes_sent;
//
//		public InstrumentedClientProxy(final InstrumentedContext instrumentedContext, Client wrap_me) {
//			wrapped = wrap_me;
//			
//			// Setup reflection to grab the pending counter.
//			try {
//				Class<? extends Client> klazz = wrap_me.getClass();
//				Field f_pendings = klazz.getDeclaredField("pendings");
//				f_pendings.setAccessible(true);
//				pendings = (AtomicLong) f_pendings.get(wrap_me);
//			} catch (Exception e) {
//				throw new RuntimeException(e);
//			}
//			
//			messages_sent = new AtomicLong();
//			bytes_sent = new AtomicLong();
//			
//			final int an_id = hashCode();
//			
//			Runnable collect = new Runnable() {
//	            @Override
//	            public void run() {
//	            	mt.send("CONN_STATS", an_id, messages_sent.get(), pendings.get(), bytes_sent.get());
//	            }
//	        };
//	        
//	        dumpStatsScheduler.scheduleWithFixedDelay(collect, 10L * 1000, 1000, TimeUnit.MILLISECONDS);
//		}
//		
//		@Override
//		public Iterator<TaskMessage> recv(int flags, int clientId) {
//			return wrapped.recv(flags, clientId);
//		}
//
//		@Override
//		public void send(int taskId, byte[] payload) {
//			wrapped.send(taskId, payload);
//			messages_sent.addAndGet(1);
//			bytes_sent.addAndGet(payload.length);
//		}
//
//		@Override
//		public void send(Iterator<TaskMessage> msgs) {
//			List<TaskMessage> msg_list = new ArrayList<TaskMessage>();
//			int msg_count = 0;
//			int byte_count = 0;
//			while (msgs.hasNext()) {
//				TaskMessage msg = msgs.next();
//				msg_list.add(msg);
//				msg_count += 1;
//				byte_count += msg.message().length;
//			}
//			
//			wrapped.send(msg_list.iterator());
//			
//			messages_sent.addAndGet(msg_count);
//			bytes_sent.addAndGet(byte_count);
//		}
//
//		@Override
//		public void close() {
//			wrapped.close();
//		}
//		
//	}
//
//	private String hostname;
//	private ScheduledExecutorService dumpStatsScheduler;
//	private IMetricsTransport mt;
//	
//	public void prepare(Map storm_conf) {
//		super.prepare(storm_conf);
//		
//		mt = MetricsTransportFactory.getInstance(storm_conf, ClassLoader.getSystemClassLoader());
//
//		try {
//			hostname = InetAddress.getLocalHost().getHostName();
//		} catch (UnknownHostException e) {
//			throw new RuntimeException(e);
//		}
//		
//		dumpStatsScheduler = Executors.newScheduledThreadPool(10);
//	}
//	
//	public IConnection connect(String storm_id, String host, int port) {
//		IConnection conn = super.connect(storm_id, host, port);
//		
//		if (mt != null) {
//			conn =  new InstrumentedClientProxy(this, (Client) conn);
//			mt.send("DECLARE_CONN", hostname, conn.hashCode(), storm_id, host, port);
//		}
//		
//		return conn;
//	}
}
