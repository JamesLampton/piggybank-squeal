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

package org.apache.pig.piggybank.squeal.flexy;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pig.piggybank.squeal.backend.storm.io.ImprovedRichSpoutBatchExecutor;
import org.apache.pig.piggybank.squeal.flexy.model.FStream;
import org.apache.pig.piggybank.squeal.flexy.topo.FlexyBolt;
import org.apache.pig.piggybank.squeal.flexy.topo.FlexyMasterSpout;
import org.jgrapht.graph.DefaultDirectedGraph;

import storm.trident.util.ErrorEdgeFactory;
import storm.trident.util.IndexedEdge;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;

public class FlexyTopology {
	DefaultDirectedGraph<FStream, IndexedEdge<FStream>> _graph;
	int index_counter = 0;
	private HashMap<FStream, FlexyBolt> boltMap;
	private DefaultDirectedGraph<FlexyBolt, IndexedEdge<FStream>> boltG;
	
	public FlexyTopology() {
		_graph = new DefaultDirectedGraph<FStream, IndexedEdge<FStream>>(new ErrorEdgeFactory());
	}
	
	private void removeMerges(DefaultDirectedGraph<FStream, IndexedEdge<FStream>> G) {
		ArrayDeque<FStream> stack = new ArrayDeque<FStream>();
		
		// First, remove any merge nodes...
		for (FStream n : G.vertexSet()) {
			if (n.getType() == FStream.NodeType.MERGE) {
				stack.add(n);
			}
		}
		
		while (stack.size() > 0) {
			FStream n = stack.pollFirst();
			
			// Move any function or projection nodes before the merge.
			for (IndexedEdge<FStream> out_edge : new ArrayList<IndexedEdge<FStream>>(G.outgoingEdgesOf(n))) {
				if (out_edge.target.getType() == FStream.NodeType.FUNCTION || out_edge.target.getType() == FStream.NodeType.PROJECTION) {					
					// Get ready to move the operation.
					FStream move_me = out_edge.target;
					// Create a new merge node.
					FStream new_merge = n.copy();
					stack.add(new_merge);
					
					for (IndexedEdge<FStream> in_edge : new ArrayList<IndexedEdge<FStream>>(G.incomingEdgesOf(n))) {
						// Link the source to the copy of the target.
						FStream prev_n = in_edge.source;
						
						// Clone the operation.
						FStream new_op = move_me.copy();
						
						// Link it to the previous chain.
						link(G, prev_n, new_op);
						// Link it to the new merge.
						link(G, new_op, new_merge);
						
						// Remove the edge.
						G.removeEdge(in_edge);
					}
					
					// Now, link the new merge
					for (IndexedEdge<FStream> other_out_edge : new ArrayList<IndexedEdge<FStream>>(G.incomingEdgesOf(move_me))) {
						// Link the new merge to the later nodes.
						link(G, new_merge, other_out_edge.target);
						
						// Remove the edge.
						G.removeEdge(other_out_edge);
					}
					
					// Remove the edge to the emptied operator.
					G.removeEdge(out_edge);
					// Remove the old op.
					G.removeVertex(move_me);
				}
			}
			
			// Then link the preceding nodes directly forward.
			for (IndexedEdge<FStream> in_edge : new ArrayList<IndexedEdge<FStream>>(G.incomingEdgesOf(n))) {
				for (IndexedEdge<FStream> out_edge : new ArrayList<IndexedEdge<FStream>>(G.outgoingEdgesOf(n))) {
					link(G, in_edge.source, out_edge.target);
				}
				G.removeEdge(in_edge);
			}
			
			// Remove the outgoing links.
			Set<IndexedEdge<FStream>> out_links = new HashSet<IndexedEdge<FStream>>(G.outgoingEdgesOf(n));
			G.removeAllEdges(out_links);			
			// Remove the empty merge node.
			G.removeVertex(n);
		}
	}
	
	private void logicalToBoltGraph() {
		// Convert the logical graph to a bolt graph.
		DefaultDirectedGraph<FStream, IndexedEdge<FStream>> G = (DefaultDirectedGraph<FStream, IndexedEdge<FStream>>) _graph.clone();
		int bolt_counter = 0;

		removeMerges(G);
		
		ArrayDeque<FStream> stack = new ArrayDeque<FStream>();
				
		// Look for the actual spouts in the graph.
		for (FStream n : G.vertexSet()) {
			if (n.getType() == FStream.NodeType.SPOUT) {
				stack.add(n);
			}
		}
		
		// State for building the topology.
		boltMap = new HashMap<FStream, FlexyBolt>();
		// The edges link specific internal nodes between the bolts -- yes, it hurts my head too.
		boltG =	new DefaultDirectedGraph<FlexyBolt, IndexedEdge<FStream>>(new ErrorEdgeFactory());
		int edge_counter = 0;

		// Track through the stack and build up subgraphs.
		while (stack.size() > 0) {
			FStream n = stack.pollFirst();
			
			FlexyBolt b = null;
			switch (n.getType()) {
			case SPOUT:
				// Create a bolt and link it into the map.
				b = new FlexyBolt(bolt_counter++, n);
				boltG.addVertex(b);
				break;
			case FUNCTION: 
			case PROJECTION:
				// Pull the previous node.
				FStream prev_n = 
						(new ArrayList<IndexedEdge<FStream>>(G.incomingEdgesOf(n)))
								.get(0).source; // Yuck.
				
				// Pull the bolt.
				b = boltMap.get(prev_n);
				// Link this node into the bolt.
				b.link(prev_n, n);
				break;
			case SHUFFLE:
				// Create a bolt and link it into the map.
				b = new FlexyBolt(bolt_counter++, n);
				boltG.addVertex(b);
				
				// Add edges for all previous nodes
				for (IndexedEdge<FStream> edge : G.incomingEdgesOf(n)) {
					FlexyBolt prev_b = boltMap.get(edge.source);
					prev_b.expose(edge.source);
					
					edge_counter += 1;
					IndexedEdge<FStream> e = new IndexedEdge<FStream>(edge.source, n, edge_counter);
					
					boltG.addEdge(prev_b, b, e);
				}
				break;
			case GROUPBY:
				// Create a new bolt for the Stage1/Store portion.
				b = new FlexyBolt(bolt_counter++, n);
				boltG.addVertex(b);
				
				// Link it to the preceding bolts.
				for (IndexedEdge<FStream> edge : G.incomingEdgesOf(n)) {
					FlexyBolt prev_b = boltMap.get(edge.source);

					// Link this node into the bolt for Stage0 Aggregation.
					FStream s0_agg = n.copy();
					s0_agg.setStage0Agg(true);
					prev_b.link(edge.source, s0_agg);
					prev_b.expose(s0_agg);
					boltMap.put(s0_agg, prev_b);
					
					edge_counter += 1;
					IndexedEdge<FStream> e = new IndexedEdge<FStream>(s0_agg, n, edge_counter);
					
					boltG.addEdge(prev_b, b, e);
				}
				break;
			default:
				throw new RuntimeException("Unexpected node type: " + n + " " + n.getType());
			}
			
			// Map the bolt.
			boltMap.put(n, b);
			
			// For all the adjacent nodes, add them to the stack.
			for (IndexedEdge<FStream> edge : G.outgoingEdgesOf(n)) {
				if (!boltMap.containsKey(edge.target)) {
					stack.addLast(edge.target);
				}
			}
		} 
	}
	
	void _visitAndBuild(FlexyBolt b, Set<FlexyBolt> built_memo, TopologyBuilder builder) {
		if (built_memo.contains(b)) {
			return;
		}
		
		// Add the bolt to the topology.
		BoltDeclarer b_builder = builder.setBolt(b.getName(), b, b.getParallelism());
		
		// Link to the appropriate inputs.
		if (b.getRoot().getType() == FStream.NodeType.SPOUT) {
			// Link to the master.
			b_builder.allGrouping("FlexyMaster", "start");
			b_builder.allGrouping("FlexyMaster", "commit");
		}
		
		for (IndexedEdge<FStream> edge : boltG.incomingEdgesOf(b)) {
			// Ensure it exists.
			FlexyBolt source_b = boltMap.get(edge.source);
			_visitAndBuild(source_b, built_memo, builder);
			
			// Now link it.
			String source_name = source_b.getName();
			String source_stream = source_b.getStreamName(edge.source);
			
			if (b.getRoot().getType() == FStream.NodeType.SHUFFLE) {
				b_builder.shuffleGrouping(source_name, source_stream);
			} else if (b.getRoot().getType() == FStream.NodeType.GROUPBY) {
				b_builder.fieldsGrouping(source_name, source_stream, b.getRoot().getGroupingFields());
			}
			
		}
	}

	public StormTopology build() {
		// Crawl the graph and create execution pipelines to be run in the bolts.
		logicalToBoltGraph();
		
		// Now, convert the bolt graph to a topology.		
		TopologyBuilder builder = new TopologyBuilder();
		
		// Create the coordinator spout.
		builder.setSpout("FlexyMaster", new FlexyMasterSpout());
		
		// Start from the leaves and walk to the spouts using DFS.
		Set<FlexyBolt> built_memo = new HashSet<FlexyBolt>();
		for (FlexyBolt b : boltG.vertexSet()) {
			if (boltG.outDegreeOf(b) == 0) {
				_visitAndBuild(b, built_memo, builder);
			}
		}
		
		return builder.createTopology();
	}
	
	public FStream merge(List<FStream> intermed) {
		// Create a new node.
		FStream n = new FStream(null, this, FStream.NodeType.MERGE);
		
		_graph.addVertex(n);
		
		for (FStream node : intermed) {
			link(node, n);
		}
		
		return n;
	}

	public FStream newStream(String name, IRichSpout spout) {
		
		// Create a new node.
		FStream n = new FStream(name, this, spout);
		
		_graph.addVertex(n);
		
		return n;
	}

	private void link(DefaultDirectedGraph<FStream, IndexedEdge<FStream>> G, FStream node, FStream n) {
		// The source should be there, the destination we'll add if necessary.
		if (!G.containsVertex(n)) {
			G.addVertex(n);
		}
		
		index_counter += 1;
		IndexedEdge<FStream> e = new IndexedEdge<FStream>(node, n, index_counter);
		G.addEdge(node, n, e);
	}
	
	public void link(FStream node, FStream n) {
		link(_graph, node, n);
	}
	
	public Set<IndexedEdge<FStream>> getIncomingEdgesOf(FStream n) {
		return _graph.incomingEdgesOf(n);
	}
}
