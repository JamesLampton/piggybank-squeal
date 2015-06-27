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

import java.util.UUID;

import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.backend.hadoop.executionengine.Launcher;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.ScriptState;
import org.apache.pig.tools.pigstats.mapreduce.MRScriptState;
import org.apache.pig.tools.pigstats.mapreduce.SimplePigStats;

public class StormExecutionEngine extends HExecutionEngine {

	public StormExecutionEngine(PigContext pigContext) {
		this(pigContext, false);
	}

	public StormExecutionEngine(PigContext pigContext, boolean run_local) {
		super(pigContext);
		this.launcher = new StormLauncher(run_local, false);
	}

	@Override
	public ScriptState instantiateScriptState() {
		// ?? This is new, may be useful...
		MRScriptState ss = new MRScriptState(UUID.randomUUID().toString());
        ss.setPigContext(pigContext);
        return ss;
	}

	@Override
	public PigStats instantiatePigStats() {
		return new SimplePigStats();
	}

}
