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

package org.apache.pig.piggybank.squeal.flexy.executors;

import java.util.Map;

import org.apache.pig.piggybank.squeal.flexy.model.FStream;
import org.jgrapht.graph.DefaultDirectedGraph;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import storm.trident.util.IndexedEdge;

public class PipelineExecutor {

	public PipelineExecutor(FStream root,
			DefaultDirectedGraph<FStream, IndexedEdge<FStream>> g) {
		// TODO Auto-generated constructor stub
	}

	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub		
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		
	}

	public void commit(Tuple input) {
		// TODO Auto-generated method stub
		
	}
}
