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

package org.apache.pig.piggybank.squeal.spout;

import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TestRateSpout extends BaseRichSpout {

	private int rate;
	private int size;
	private byte[] buf;
	private long lastMinute;
	private int curSent;
	private SpoutOutputCollector collector;
	private Random r;
	private static final Log log = LogFactory.getLog(TestRateSpout.class);
	int div = 60000;

	public TestRateSpout(String ratePerSecond, String sizeInBytes, String perTime) {
		rate = Integer.parseInt(ratePerSecond);
		size = Integer.parseInt(sizeInBytes);
		div = Integer.parseInt(perTime);
	}
	
	public TestRateSpout(String ratePerSecond, String sizeInBytes) {
		this(ratePerSecond, sizeInBytes, "60000");
	}
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		buf = new byte[size];
		for (int i = 0; i < buf.length; i++) {
			buf[i] = (byte) (i % 256);
		}
		lastMinute = 0;
		curSent = 0;
		this.collector = collector;
		r = new Random();
	}

	@Override
	public void nextTuple() {
		long now = System.currentTimeMillis() / div;
		
		if (now != lastMinute) {
			log.info("sent " + curSent + " of " + rate + " during " + lastMinute);
			lastMinute = now;
			curSent = 0;

		}
		
		// We've emitted all we should.
		if (curSent >= rate) {
			return;
		}
		curSent += 1;
		
		// Emit a record.
		collector.emit(new Values(Long.toString(lastMinute).getBytes(), Long.toString(curSent).getBytes(), buf), r.nextInt());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("lastMinute", "curSent", "buf"));
	}

}
