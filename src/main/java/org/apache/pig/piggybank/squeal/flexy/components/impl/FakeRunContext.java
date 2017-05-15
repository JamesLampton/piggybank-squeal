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
package org.apache.pig.piggybank.squeal.flexy.components.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.pig.piggybank.squeal.flexy.components.IRunContext;
import org.apache.pig.piggybank.squeal.flexy.model.FFields;
import org.apache.pig.piggybank.squeal.flexy.model.FStream;

import backtype.storm.task.IMetricsContext;
import backtype.storm.task.TopologyContext;

public class FakeRunContext implements IRunContext {
	@Override
	public int getThisTaskId() {
		return 0;
	}

	@Override
	public String getThisComponentId() {
		return null;
	}

	@Override
	public int getThisTaskIndex() {
		return 0;
	}

	@Override
	public String getStormId() {
		return null;
	}

	@Override
	public int getPartitionIndex() {
		return 0;
	}

	@Override
	public Object get(String key) {
		return null;
	}

	@Override
	public String getExposedName(FStream cur) {
		return null;
	}

	@Override
	public Map getStormConf() {
		return new HashMap();
	}

	@Override
	public TopologyContext getStormTopologyContext() {
		return null;
	}

	@Override
	public FFields getInputSchema() {
		return null;
	}

	@Override
	public void runWaitStrategy(int emptyStreak) {
		
	}

	@Override
	public int getNumPartitions() {
		return 1;
	}

	@Override
	public IMetricsContext getMetricsContext() {
		return null;
	}
}
