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

package org.apache.pig.piggybank.squeal.backend.storm;

import java.lang.reflect.Field;
import java.util.Map.Entry;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.impl.PigContext;

import com.twitter.heron.api.generated.TopologyAPI.StreamId;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.TopologyContext;

public class MonkeyPatch {
	
	public static void PigContextRefreshEngine(PigContext pc) {
		/* Patch for PigContext:
		 * 
		 * +    public void refreshExecutionEngine() {
         * +    	executionEngine = execType.getExecutionEngine(this);
         * +    }
		 */
		
		// We're going to use reflection to hack this one out.
		try {
			Class<? extends PigContext> klazz = pc.getClass();
			Field executionEngine = klazz.getDeclaredField("executionEngine");
			executionEngine.setAccessible(true);
			executionEngine.set(pc, pc.getExecType().getExecutionEngine(pc));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static void MapReduceOperSetRequestedParallelism(
			MapReduceOper state_mr, int requestedParallelism) {
		/* Patch for MapReduceOper:
		 * 
		 * +    public void setRequestedParallelism(int rp) {
		 * +        requestedParallelism = rp;
		 * +    }
		 */
		
		// We're going to use reflection to hack this one out.
		try {
			Class<? extends MapReduceOper> klazz = state_mr.getClass();
			Field f_rp = klazz.getDeclaredField("requestedParallelism");
			f_rp.setAccessible(true);
			f_rp.set(state_mr, requestedParallelism);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}		
	}
	
//	public static TopologyContext getTopologyContext(TridentOperationContext tri_context) {
//		try {
//			Class<? extends TridentOperationContext> klazz = tri_context.getClass();
//			Field f_other_context = klazz.getDeclaredField("_topoContext");
//			f_other_context.setAccessible(true);;
//			return (TopologyContext) f_other_context.get(tri_context);
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//	}
	
	public static com.twitter.heron.api.topology.TopologyContext getTopologyContextDelegate(TopologyContext context) {
		// For Heron, we need to dig into the topology context to grab the hidden delegate.
		com.twitter.heron.api.topology.TopologyContext delegate = null;
		try {
			Class<? extends TopologyContext> klazz = context.getClass();
			Field f_delegate = klazz.getDeclaredField("delegate");
			f_delegate.setAccessible(true);
			return (com.twitter.heron.api.topology.TopologyContext) f_delegate.get(context);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static int getCoordCount(TopologyContext context) {
		// For Heron, we need to dig into the topology context to grab the hidden delegate.
		com.twitter.heron.api.topology.TopologyContext delegate = getTopologyContextDelegate(context);
		
		int c = 0;
		for (Entry<StreamId, com.twitter.heron.api.generated.TopologyAPI.Grouping> prev : delegate.getSources(context.getThisComponentId()).entrySet()) {
			if (prev.getKey().getId().equals("coord")) {
				c += context.getComponentTasks(prev.getKey().getId()).size();
			}
//			log.info(getName() + " || " + prev.getKey().get_streamId() + " ---> " + context.getThisComponentId());
		}
		return c;
	}
}
