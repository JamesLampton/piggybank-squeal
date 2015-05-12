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

import org.apache.pig.piggybank.squeal.backend.storm.io.ImprovedRichSpoutBatchExecutor;
import org.apache.pig.piggybank.squeal.flexy.FlexyTopology;

import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.Function;
import storm.trident.state.StateFactory;
import storm.trident.util.TridentUtils;
import backtype.storm.tuple.Fields;

public class FStream {
	
	private String name;
	private FlexyTopology parent;
	private NodeType nodeType;
	private ImprovedRichSpoutBatchExecutor spout;
	private int parallelismHint;
	private Fields output_fields;
	private Fields input_fields;
	private Function func;
	private boolean stage0Agg = false;
	private Fields group_key;
	private CombinerAggregator stage1Agg;
	private CombinerAggregator stage2Agg;
	private CombinerAggregator storeAgg;
	private StateFactory sf;
	private Fields func_project;
	
	public enum NodeType {
		SPOUT, FUNCTION, PROJECTION, SHUFFLE, GROUPBY, MERGE
	}

	public FStream(String name, FlexyTopology parent, NodeType t) {
		this.nodeType = t;

		this.name = name;
		this.parent = parent;
	}

	public FStream(String name, FlexyTopology parent,
			ImprovedRichSpoutBatchExecutor spout) {
		this(name, parent, NodeType.SPOUT);
		this.setSpout(spout);
	}

	public FStream name(String name) {
		this.name = name;
		return this;
	}

	public void parallelismHint(int parallelismHint) {
		this.parallelismHint = parallelismHint;
	}

	public FStream each(Fields input, Function func,
			Fields output) {
		// Create a function node.
		FStream n = new FStream(null, parent, NodeType.FUNCTION);
		
		// Set the function information.
		n.setFunctionInformation(getOutputFields(), input, func, output);
		
		// Link the nodes.
		parent.link(this, n);
		
		return n;
	}

	public FStream project(Fields output) {
		// Create a project node.
		FStream n = new FStream(null, parent, NodeType.PROJECTION);

		// Set the function information.
		n.setProjection(getOutputFields(), output);

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

	public FStream groupBy(Fields group_key, 
			CombinerAggregator stage1Agg, 
			CombinerAggregator stage2Agg, 
			CombinerAggregator storeAgg, 
			StateFactory sf, 
			Fields output_fields) {
		
		// Create a groupby node.
		FStream n = new FStream(null, parent, NodeType.GROUPBY);

		n.setGroupBySpec(group_key, stage1Agg, stage2Agg, storeAgg, sf, output_fields);
		
		// Link the nodes.
		parent.link(this, n);

		return n;
	}
	
	public Fields getOutputFields() {
		switch (nodeType) {
		case FUNCTION:
			return TridentUtils.fieldsConcat(input_fields, output_fields);
		case GROUPBY:
			return TridentUtils.fieldsConcat(group_key, output_fields);
		case MERGE:
		case SHUFFLE:
			// Pull a predecessor.
			return ((FStream[]) 
					parent.getIncomingEdgesOf(this).toArray())[0]
							.getGroupingFields();
		case PROJECTION:
			return output_fields;
		case SPOUT:
			return spout.getOutputFields();
		default:
			throw new RuntimeException("Unknown node type:" + nodeType);
		}
	}
	
	private void setSpout(ImprovedRichSpoutBatchExecutor spout) {
		this.spout = spout;
	}

	private void setFunctionInformation(Fields input, Fields project, 
			Function func, Fields output) {
		input_fields = input;
		func_project = project;
		this.func = func;
		output_fields = output;
	}

	private void setProjection(Fields input, Fields output) {
		this.input_fields = input;
		this.output_fields = output;
		
		// Ensure that the fields in output are present in the input.
		for (String field : output.toList()) {
			if (!input.contains(field)) {
				throw new RuntimeException("Missing field [" + field + 
						"] in projecting " + input + " to " + output);
			}
		}
	}

	private void setGroupBySpec(Fields group_key, CombinerAggregator stage1Agg,
			CombinerAggregator stage2Agg, CombinerAggregator storeAgg, 
			StateFactory sf, Fields output_fields) {
		
		this.group_key = group_key;
		this.stage1Agg = stage1Agg;
		this.stage2Agg = stage2Agg;
		this.storeAgg = storeAgg;
		this.sf = sf;
		this.output_fields = output_fields;
	}

	public NodeType getType() {
		return nodeType;
	}

	public FStream copy() {
		FStream n = new FStream(null, parent, nodeType);
		
		switch (nodeType) {
		case FUNCTION:
			n.setFunctionInformation(input_fields, func_project, func, output_fields);
			break;
		case GROUPBY:
			n.setGroupBySpec(group_key, stage1Agg, stage2Agg, storeAgg, sf, output_fields);
			n.setStage0Agg(stage0Agg);
			break;
		case MERGE:
		case SHUFFLE:
			// This is really a placeholder node...
			break;
		case PROJECTION:
			n.setProjection(input_fields, output_fields);
			break;
		case SPOUT:
			n.setSpout(spout);
			break;
		default:
			throw new RuntimeException("Unknown node type:" + nodeType);
		}
		
		n.parallelismHint(parallelismHint);
		n.name(name);
		
		return n;
	}

	public void setStage0Agg(boolean b) {
		stage0Agg  = b;
	}

	public Fields getGroupingFields() {
		return this.group_key;
	}

	public String getName() {
		return name;
	}

	public int getParallelism() {
		return parallelismHint;
	}
}
