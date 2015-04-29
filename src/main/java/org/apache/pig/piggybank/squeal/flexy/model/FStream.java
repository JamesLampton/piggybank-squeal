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

package org.apache.pig.piggybank.squeal.flexy.model;

import java.util.UUID;

import org.apache.pig.piggybank.squeal.backend.storm.io.ImprovedRichSpoutBatchExecutor;
import org.apache.pig.piggybank.squeal.flexy.FlexyTopology;

import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.Function;
import storm.trident.state.StateFactory;
import backtype.storm.tuple.Fields;

public class FStream {
	
	private String name;
	private FlexyTopology parent;
	private String nodeId;
	private NodeType nodeType;
	private ImprovedRichSpoutBatchExecutor spout;
	private int parallelismHint;
	
	public enum NodeType {
		SPOUT, FUNCTION, PROJECTION, SHUFFLE, GROUPBY
	}

	public FStream(String name, FlexyTopology parent, NodeType t) {
		this.nodeId = UUID.randomUUID().toString();
		this.nodeType = t;

		this.name = name;
		this.parent = parent;
	}

	public FStream name(String name) {
		this.name = name;
		return this;
	}

	public void parallelismHint(int parallelismHint) {
		this.parallelismHint = parallelismHint;
	}

	public Fields getOutputFields() {
		// TODO
		return null;
	}

	public FStream each(Fields input, Function func,
			Fields output) {
		// Create a function node.
		FStream n = new FStream(null, parent, NodeType.FUNCTION);
		
		// Set the function information.
		n.setFunctionInformation(input, func, output);
		
		// Link the nodes.
		parent.link(this, n);
		
		return n;
	}

	public FStream project(Fields output) {
		// Create a project node.
		FStream n = new FStream(null, parent, NodeType.PROJECTION);

		// Set the function information.
		n.setProjection(output);

		// Link the nodes.
		parent.link(this, n);

		return n;
	}

	public FStream shuffle() {
		// Create a shuffle node.
		FStream n = new FStream(null, parent, NodeType.SHUFFLE);
		
		// Link the nodes.
		parent.link(this, n);

		return n;
	}

	public FStream groupBy(Fields group_key, CombinerAggregator stage1Agg, CombinerAggregator stage2Agg, StateFactory sf) {
		// Create a groupby node.
		FStream n = new FStream(null, parent, NodeType.GROUPBY);

		n.setGroupBySpec(group_key, stage1Agg, stage2Agg, sf);
		
		// Link the nodes.
		parent.link(this, n);

		return n;
	}
	
	public void setSpout(ImprovedRichSpoutBatchExecutor spout) {
		this.spout = spout;
	}

	private void setFunctionInformation(Fields input, Function func,
			Fields output) {
		// TODO Auto-generated method stub
	}

	private void setProjection(Fields output) {
		// TODO Auto-generated method stub
	}

	private void setGroupBySpec(Fields group_key, CombinerAggregator stage1Agg,
			CombinerAggregator stage2Agg, StateFactory sf) {
		// TODO Auto-generated method stub
	}
}
