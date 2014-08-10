package org.apache.pig.backend.storm;

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
		this.launcher = new StormLauncher(run_local);
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
