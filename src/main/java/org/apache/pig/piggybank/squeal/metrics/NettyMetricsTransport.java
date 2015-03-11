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
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
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

public class NettyMetricsTransport extends AbstractMetricsTransport {

	static public final String NETTY_HOST_KEY = "pig.streaming.metrics.transport.netty.host";
	static public final String NETTY_PORT_KEY = "pig.streaming.metrics.transport.netty.port";
	
	static public final int DEFAULT_PORT = 9988;
	private Channel channel;
	
	public void initialize(PigContext pc) {
		initialize(pc.getProperties());
	}
	
	@Override
	public void initialize(Map props) {
		super.initialize(props);
		
		// Pull the destination host and port.
		String host = props.get(NETTY_HOST_KEY).toString();
		Object p_string = props.get(NETTY_PORT_KEY);
		int port = DEFAULT_PORT;
		if (p_string != null) {
			port = Integer.parseInt(p_string.toString());
		}
			
		// Connect and pull the channel.
		ChannelFactory factory =
				new NioClientSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool());

		ClientBootstrap bootstrap = new ClientBootstrap(factory);

		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() {
				return Channels.pipeline(
						new StringEncoder(CharsetUtil.UTF_8), 
						new SimpleChannelHandler());
			}
		});

		bootstrap.setOption("keepAlive", true);

		ChannelFuture f = bootstrap.connect(new InetSocketAddress(host, port));
		
		f.awaitUninterruptibly();
		if (!f.isSuccess()) {
			throw new RuntimeException("Initialization failed.", f.getCause());
		}
		
		channel = f.getChannel();
	}

	@Override
	public void send(byte[] buf) {
		// Pad with newline if necessary.
		if (buf[buf.length-1] != '\n') {
			buf = Arrays.copyOf(buf, buf.length + 1);
			buf[buf.length - 1] = '\n';
		}
		channel.write(new String(buf));
	}
	
	class NettyMetricsHandler extends SimpleChannelHandler {
//		ByteBuffer bbuf = ByteBuffer.allocate(64*1024);
		private BlockingQueue<String> q;

		NettyMetricsHandler(BlockingQueue<String> q) {
			this.q = q;
		}
		
		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
	       String msg = (String) e.getMessage();
	       
	       q.put(msg);
	    }
		
		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
			e.getCause().printStackTrace();
			Channel ch = e.getChannel();
			ch.close();
		}
	}

	private void runServer(int port, final OutputStream os) throws Exception {
		ChannelFactory factory = new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		
		ServerBootstrap bootstrap = new ServerBootstrap(factory);		
		
		// Check args for port and override on output stream.
		
		final BlockingQueue<String> q = new LinkedBlockingQueue<String>();
		
		final Thread dumper = new Thread() {
			public void run() {
				ArrayList<String> drainToMe = new ArrayList<String>();
				
				while (true) {
					try {
						drainToMe.clear();
						q.drainTo(drainToMe, 1024);
						
						for (String buf : drainToMe) {
							os.write(buf.getBytes());
							os.flush();
						}
					} catch (Exception e) {
						
					}
				}
				
			}
		};
		
		dumper.start();
		
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() {
				return Channels.pipeline(
							new DelimiterBasedFrameDecoder(64*1024, false, Delimiters.lineDelimiter()),
							new StringDecoder(CharsetUtil.UTF_8),
							new NettyMetricsHandler(q)
						);
			}
		});
		
//		bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.bind(new InetSocketAddress(port));
		
	}
	
	public static void main(String args[]) throws Exception {
		boolean runServer = false;
		int options_start = 0;
		
		if (args.length == 0) {
			runServer = true;
		} else {
			if (args[0].equals("-runServer")) {
				options_start += 1;
				runServer = true;
			} else if (args[0].equals("-transmit")) {
				options_start += 1;
			} else {
				System.err.println("Unknown option: " + args[0]);
				return;
			}
		}
		
		
		if (runServer) {
			
			String destination = "<stdout>";
			final OutputStream os;
			int port = DEFAULT_PORT;
			if (args.length > options_start) {
				port = Integer.parseInt(args[options_start]);
			}
			if (args.length > options_start + 1) {
				destination = args[options_start + 1];
				os = new FileOutputStream(destination);
			} else {
				os = System.out;
			}

			System.err.println("Starting server on port: " + port + " writing to: " + destination);
		
			NettyMetricsTransport mt = new NettyMetricsTransport();
			mt.runServer(port, os);
		} else if (args.length > 0 && args[0].equals("-transmit")) {
			
			String destination = "127.0.0.1";
			int port = DEFAULT_PORT;
			if (args.length > options_start) {
				destination = args[options_start];
			}
			if (args.length > options_start + 1) {
				port = Integer.parseInt(args[options_start + 1]);
			}
			

			System.err.println("Sending to " + destination + ":" + port);
			
			Properties p = new Properties();
			p.put(MetricsTransportFactory.METRICS_TRANSPORT_KEY, NettyMetricsTransport.class.getName());
			p.put(NETTY_HOST_KEY, destination);
			p.put(NETTY_PORT_KEY, Integer.toString(port));
			
			IMetricsTransport mt = MetricsTransportFactory.getInstance(p, NettyMetricsTransport.class.getClassLoader());
			
			BufferedReader bi = new BufferedReader(new InputStreamReader(System.in));
			String line;
			while ((line = bi.readLine()) != null) {
				System.out.println("SENDING: " + line);
				mt.send(line.getBytes());
			}
			
		}
	}
}
