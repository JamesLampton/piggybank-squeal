/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pig.piggybank.squeal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.FixedTupleSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class TestSentenceSpout2 extends TestSentenceSpout {
	
	static  List<List<byte[]>> test_tuples2;
	static {
		test_tuples2 = new ArrayList<List<byte[]>>();
        test_tuples2.add(getSentList("hello world"));
        test_tuples2.add(getSentList("more cowbell"));
        test_tuples2.add(getSentList("are these even sentences?"));
        test_tuples2.add(getSentList("it was the best of times it was the worst of times"));
        test_tuples2.add(getSentList("a rose is a rose is a rose"));
        test_tuples2.add(getSentList("my mother is a fish"));
	}
	
    public TestSentenceSpout2() {
        super(test_tuples2);
	}
}
