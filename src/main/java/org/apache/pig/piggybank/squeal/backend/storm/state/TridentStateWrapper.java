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

package org.apache.pig.piggybank.squeal.backend.storm.state;

import java.io.Serializable;
import java.util.List;

import org.apache.pig.piggybank.squeal.flexy.components.IMapState;
import org.apache.pig.piggybank.squeal.flexy.components.IRunContext;
import org.apache.pig.piggybank.squeal.flexy.components.IStateFactory;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.MapState;

public class TridentStateWrapper implements IStateFactory, Serializable {

	private StateFactory stateFactory;

	public TridentStateWrapper(StateFactory stateFactory) {
		this.stateFactory = stateFactory;
	}

	@Override
	public IMapState makeState(IRunContext context) {
		return new TridentMapWrapper((MapState) stateFactory.makeState(context.getStormConf(), context.getMetricsContext(), context.getPartitionIndex(), context.getNumPartitions()));
	}

	static class TridentMapWrapper implements IMapState {

		private MapState wrapped;

		public TridentMapWrapper(MapState mapState) {
			wrapped = mapState;
		}

		@Override
		public List multiGet(List keys) {
			return wrapped.multiGet(keys);
		}

		@Override
		public List multiUpdate(List keys, List updaters) {
			return wrapped.multiUpdate(keys, updaters);
		}

		@Override
		public void multiPut(List keys, List vals) {
			wrapped.multiPut(keys, vals);
		}

		@Override
		public void commit(long txid) {
			wrapped.commit(txid);
		}
		
	}
	
}
