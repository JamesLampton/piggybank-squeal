package org.apache.pig.backend.storm.oper;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigLogger;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigProgressable;
import org.apache.pig.backend.storm.Main;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.impl.PigContext;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentOperationContext;

public abstract class StormBaseFunction extends BaseFunction {

	private PigContext pc;

	public StormBaseFunction(PigContext pc) {
		this.pc = pc;
	}
	
	private static final Log log = LogFactory.getLog(StormBaseFunction.class);

	class DummyProgress implements PigProgressable {
		@Override
		public void progress() {
			
		}
		@Override
		public void progress(String msg) {
			
		}
	}
	
	class DummyLogger implements PigLogger {
		@Override
		public void warn(Object o, String msg, Enum warningEnum) {
			System.err.println(o.toString() + " " + msg);
//			log.warn(o.toString() + " " + msg);
		}
	}
	
	public void	prepare(Map conf, TridentOperationContext context) {
		super.prepare(conf, context);
		
		try {
			pc.refreshExecutionEngine();
			pc.connect();
			
			// FIXME: Try an empty configuration.
			SchemaTupleBackend.initialize(new Configuration(), pc);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		PhysicalOperator.setReporter(new DummyProgress());
		PhysicalOperator.setPigLogger(new DummyLogger());		
	}
}
