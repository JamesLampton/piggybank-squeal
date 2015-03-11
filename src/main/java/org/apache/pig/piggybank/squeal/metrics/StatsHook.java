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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import backtype.storm.hooks.ITaskHook;
import backtype.storm.hooks.info.BoltAckInfo;
import backtype.storm.hooks.info.BoltExecuteInfo;
import backtype.storm.hooks.info.BoltFailInfo;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.hooks.info.SpoutFailInfo;
import backtype.storm.task.TopologyContext;

public class StatsHook implements ITaskHook {
	
	private IMetricsTransport mt;
	private int taskId = -1;
	private int taskIdx = -1;
	private String compId = "unknown";
	private String stormId = "unknown";
	private Object hostname = "unknown";
	private Integer worker_port;

	@Override
	public void prepare(Map conf, TopologyContext context) {
		this.mt = MetricsTransportFactory.getInstance(conf, ClassLoader.getSystemClassLoader());
		
		// Pull the component name and any other information from the conf.
		try { 
			taskId = context.getThisTaskId();
			taskIdx = context.getThisTaskIndex();
			compId = context.getThisComponentId();
			stormId = (String) conf.get("storm.id");
			hostname = InetAddress.getLocalHost().getHostName();
			worker_port = context.getThisWorkerPort();
		} catch (Exception e) {
			// Leave things as unknown on error.
		}
		
		// Send an initial declare message.
		send("DECL", hostname, stormId, taskId, taskIdx, compId, worker_port);	
	}

	@Override
	public void cleanup() {
		this.mt = null;
	}
	
	void send(Object... msg) {
		StringBuilder sb = new StringBuilder();

		sb.append(System.currentTimeMillis());

		for (Object o : msg) {
			sb.append("\t");
			sb.append(o == null ? "" : o.toString());
		}
		sb.append("\n");

		mt.send(sb.toString().getBytes());
	}
	
	@Override
	public void emit(EmitInfo info) {
		send("EMIT", info.taskId, info.stream, info.outTasks.toString());
	}

	@Override
	public void spoutAck(SpoutAckInfo info) {
		send("SACK", info.spoutTaskId, info.completeLatencyMs, info.messageId);
	}

	@Override
	public void spoutFail(SpoutFailInfo info) {
		send("SFAIL", info.spoutTaskId, info.failLatencyMs, info.messageId);
		
	}

	@Override
	public void boltExecute(BoltExecuteInfo info) {
		send("BEXEC", info.executingTaskId, info.executeLatencyMs);
	}

	@Override
	public void boltAck(BoltAckInfo info) {
		send("BACK", info.ackingTaskId, info.processLatencyMs);
	}

	@Override
	public void boltFail(BoltFailInfo info) {
		send("BACK", info.failingTaskId, info.failLatencyMs);
	}

}
