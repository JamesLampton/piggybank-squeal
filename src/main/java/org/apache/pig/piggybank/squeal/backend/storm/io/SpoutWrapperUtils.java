package org.apache.pig.backend.storm.io;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.IRichSpout;

public class SpoutWrapperUtils {
	
	static public class AutoID implements Serializable {
		int rand_id;
		
		public AutoID() {};
		public AutoID(int an_id) { rand_id = an_id; }
	}
	
	static public class AutoIDCollector implements ISpoutOutputCollector {
		private final Log log = LogFactory.getLog(getClass());
		private ISpoutOutputCollector proxied;
		Random _rand;
		
		public static SpoutOutputCollector newInstance(ISpoutOutputCollector obj) {			
			return new SpoutOutputCollector(new AutoIDCollector(obj));
		}
		
		Object fixId(Object id) {
			if (id != null) {
				return id;
			}
			
			return new AutoID(_rand.nextInt());
		}
		
		public AutoIDCollector (ISpoutOutputCollector obj) {
			super();
			proxied = obj;
			_rand = new Random();
		}
		
		@Override
		public List<Integer> emit(String stream, List<Object> values, Object id) {
			return proxied.emit(stream, values, fixId(id));
		}

		@Override
		public void emitDirect(int task, String stream, List<Object> values, Object id) {
			proxied.emitDirect(task, stream, values, fixId(id));
		}

		@Override
		public void reportError(Throwable t) {
			proxied.reportError(t);
		}
		
		
	}
	
	static public class LimitedOutstandingInvocationHandler implements InvocationHandler, Serializable {
		private final Log log = LogFactory.getLog(getClass());
		final String CONF_OVERRIDE = "pig.streaming.spout.max.outstanding";
		
		public static IRichSpout newInstance(IRichSpout obj) {			
			return (IRichSpout) Proxy.newProxyInstance(
					obj.getClass().getClassLoader(),
					new Class[] { IRichSpout.class, Serializable.class },
					new LimitedOutstandingInvocationHandler(obj));
		}
		
		transient Semaphore resources;
		int resource_count = 999;
		private IRichSpout proxied;
		
		public LimitedOutstandingInvocationHandler(IRichSpout spout) {
			this(spout, 0);
		}
		
		public LimitedOutstandingInvocationHandler(IRichSpout spout, int resource_count) {
			this.proxied = spout;
			if (resource_count > 0) {
				this.resource_count = resource_count;
			}
		}
		
		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
//			log.info(method.getName());
//			if (args != null) {
//				log.info(Arrays.asList(args));
//			}
			
			if (method.getName().equals("open")) {
				// FIXME: Check on emit and determine if we need to intercept and
				// provide a default id mechanism.
				
				// Pull any override values.
				Map conf = (Map) args[0];
				if (conf.containsKey(CONF_OVERRIDE)) {
					resource_count = Integer.parseInt((String) conf.get(CONF_OVERRIDE));
				}
				
				// Replace the collector
				args[2] = AutoIDCollector.newInstance((ISpoutOutputCollector) args[2]);

				log.info("Configuring " + resource_count + " oustanding resources for consumption.");

				// Initialize our resource semaphore.
				resources = new Semaphore(resource_count);
			} else if (method.getName().equals("nextTuple")) {
				// Attempt to acquire some resources.
				if (!resources.tryAcquire()) {
					// No free resources, bounce.
					return null;
				}
//				log.info("ACQUIRED");
			} else if (method.getName().equals("ack") || method.getName().equals("fail")) {
//				log.info("RELEASE");
				// Release a resource.
				resources.release();
				
				if (args[0] != null && args[0] instanceof AutoID) {
					args[0] = null;
				}
			}
			
			return method.invoke(proxied, args);
		}
	}
}
