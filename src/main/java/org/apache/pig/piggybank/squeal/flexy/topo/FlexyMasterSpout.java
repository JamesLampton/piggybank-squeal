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

package org.apache.pig.piggybank.squeal.flexy.topo;

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

public class FlexyMasterSpout extends BaseRichSpout {
	private static final Log log = LogFactory.getLog(FlexyMasterSpout.class);

	private SpoutOutputCollector collector;
	int cur_state = 0;
	Long cur_batch = 0L;
	boolean last_failed = false;
	long start_ts = 0;
	Random r;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		r = new Random();
	}

	@Override
	public void nextTuple() {
		int enter_state = cur_state;
		
		if (cur_state == 0) {
			// Start a new batch.
			collector.emit("start", new Values(cur_batch, last_failed), r.nextInt());
			last_failed = false;
			long now = System.currentTimeMillis();
			if (start_ts != 0 && (now-start_ts) > 10000) {
				System.err.println("Batch: " + cur_batch + " " + (now-start_ts) + " ms" );
			}
			start_ts = now;
			
			cur_batch ++;
			cur_state ++;
		} else if (cur_state == 1) {
			// Waiting for propagation of batch..
		} else if (cur_state == 2) {
			// Batch completed, begin commit.
			collector.emit("commit", new Values(cur_batch, true), r.nextInt());
			cur_state ++;
		} else if (cur_state == 3) {
			// Waiting for commit complete.
		} else if (cur_state == 4) {
			// Commit failed, roll back.
			collector.emit("commit", new Values(cur_batch, false), r.nextInt());
			cur_state ++;
		} else {
			// Do nothing.
		}
		
		if (cur_state != enter_state) {
//			log.info(cur_batch + " switched: " + enter_state + " -> " + cur_state);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("start", new Fields("batchid", "last_failed"));
		declarer.declareStream("commit", new Fields("batchid", "success"));
	}

	@Override
    public void ack(Object msgId) {
//		log.info(cur_batch + " ack: " + cur_state);
		// Successful complete drops us back to go.
		if (cur_state >= 3) {
			cur_state = 0;
		} else {
			cur_state ++;
		}
    }

    @Override
    public void fail(Object msgId) {
    	log.info(cur_batch + " failed: " + cur_state);
    	// We need to switch to a failed state.
    	cur_state = 4;
    	last_failed = true;
    }
}
