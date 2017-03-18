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

package org.apache.pig.piggybank.squeal.backend.storm.io;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.pig.piggybank.squeal.flexy.components.ICollector;
import org.apache.pig.piggybank.squeal.metrics.IMetricsTransport;
import org.apache.pig.piggybank.squeal.metrics.MetricsTransportFactory;

import backtype.storm.Config;
import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.RotatingMap;
import backtype.storm.utils.Utils;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;
import storm.trident.util.TridentUtils;

// Taken mostly from RichSpoutBatchExecutor.
public class ImprovedRichSpoutBatchExecutor implements ITridentSpout {
	public static final String MAX_BATCH_SIZE_CONF = "topology.spout.max.batch.size";
	static public final String RATE_BASE_PATH_KEY = "topology.spout.max.batch.size.zkpath";
	static public final int DEFAULT_MAX_BATCH_SIZE = 1000;

    IRichSpout _spout;
    
    public ImprovedRichSpoutBatchExecutor(IRichSpout spout) {
        _spout = spout;
    }

    @Override
    public Map getComponentConfiguration() {
        return _spout.getComponentConfiguration();
    }

    @Override
    public Fields getOutputFields() {
        return TridentUtils.getSingleOutputStreamFields(_spout);
        
    }

    @Override
    public BatchCoordinator getCoordinator(String txStateId, Map conf, TopologyContext context) {
        return new RichSpoutCoordinator();
    }

    @Override
    public Emitter getEmitter(String txStateId, Map conf, TopologyContext context) {
        return new RichSpoutEmitter(conf, context);
    }
    
    static CuratorFramework newCuratorStarted(Map conf, List<String> servers, Object port) {
    	// Based on Utils.newCuratorStarted in Storm
    	List<String> serverPorts = new ArrayList<String>();
        for(String zkServer: (List<String>) servers) {
            serverPorts.add(zkServer + ":" + Utils.getInt(port));
        }
        String zkStr = StringUtils.join(serverPorts, ",") + "";
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                .connectString(zkStr)
                .connectionTimeoutMs(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT)))
                .sessionTimeoutMs(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)))
                .retryPolicy(new BoundedExponentialBackoffRetry(
                            Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL)),
                            Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING)),
                            Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES))));
        
        return builder.build();
    }
    
    public static void main(final String args[]) throws Exception {
    	if (args.length == 0) {
    		System.out.println("usage: " + ImprovedRichSpoutBatchExecutor.class.getName() + " path [-put <value>]");
    		return;
    	}

    	String path = args[0];
    	
    	boolean get = true;
    	if (args.length > 1 && args[1].equals("-put")) {
    		get = false;
    	}
    	
    	Map conf = Utils.readStormConfig();
    	List<String> servers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        Object port = conf.get(Config.STORM_ZOOKEEPER_PORT);

    	System.out.println("Getting curator: " + servers + " " + port);
    	CuratorFramework curator = ImprovedRichSpoutBatchExecutor.newCuratorStarted(conf, servers, port);
    	SharedCount c = new SharedCount(curator, path, -1);
    	c.start();
    	
    	if (get) {
    		System.out.println(path + " == " + c.getCount());
    	} else {
    		int set_to = Integer.parseInt(args[2]);
    		System.out.println("Setting " + path + " to " + set_to);
    		c.setCount(set_to);
    	}
    	
    	System.exit(0);
    }
    
    class RichSpoutEmitter implements ITridentSpout.Emitter<Object> {
        AtomicInteger _maxBatchSize;
        boolean prepared = false;
        CaptureCollector _collector;
        RotatingMap<Long, List<Object>> idsMap;
        Map _conf;
        TopologyContext _context;
        long lastRotate = System.currentTimeMillis();
        long rotateTime;
		private IMetricsTransport mt;
		private int taskId;
		private int taskIdx;
		private String compId;
		private String stormId;
		private String hostname;
		private CuratorFramework _curator;
		private SharedCount _count;

        public RichSpoutEmitter(Map conf, TopologyContext context) {
            _conf = conf;
            _context = context;
            Number batchSize = (Number) conf.get(MAX_BATCH_SIZE_CONF);
            if(batchSize==null) batchSize = DEFAULT_MAX_BATCH_SIZE;
            _maxBatchSize = new AtomicInteger(batchSize.intValue());
            _collector = new CaptureCollector();
            idsMap = new RotatingMap(3);
            rotateTime = 1000L * ((Number)conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)).intValue();
            
            // Pull a metrics transport if configured.
    		mt = MetricsTransportFactory.getInstance(conf, ClassLoader.getSystemClassLoader());
    		
    		try {
    			taskId = context.getThisTaskId();
    			taskIdx = context.getThisTaskIndex();
    			compId = context.getThisComponentId();
    			stormId = (String) conf.get("storm.id");
    			hostname = InetAddress.getLocalHost().getHostName();
    		} catch (Exception e) {
    			// Leave things as unknown on error.
    		}
    		
    		// Create a curator to read the max batch size.
    		String rate_path = (String) conf.get(RATE_BASE_PATH_KEY);
    		if (rate_path != null) {
    			String rootDir = rate_path + "/" + context.getStormId();
                List<String> servers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
                Object port = conf.get(Config.STORM_ZOOKEEPER_PORT);
                
                _curator = newCuratorStarted(conf, servers, port);;
                
                // Register and listen to the shared int.
                _count = new SharedCount(_curator, rootDir + "/" + compId, _maxBatchSize.get());
                try {
                	_count.start();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
                
                _count.addListener(new SharedCountListener() {
					@Override
					public void stateChanged(CuratorFramework client,
							ConnectionState newState) {
					}

					@Override
					public void countHasChanged(SharedCountReader c, int val)
							throws Exception {
						System.out.println(compId + " updating max batch count to " + val);
						_maxBatchSize.set(val);
						
						if (mt != null) {
							mt.send("EMITTER_MAX_SIZE", taskId, compId, _maxBatchSize.get());
						}
					}	
                });
    		}
    		
    		// send a declare message.
    		if (mt != null) {
    			mt.send("DECL_EMITTER", hostname, stormId, taskId, taskIdx, compId, hashCode(), _spout.toString());
    			mt.send("EMITTER_MAX_SIZE", taskId, compId, _maxBatchSize.get());
    		}
    		
        }
        
        @Override
        public void emitBatch(TransactionAttempt tx, Object coordinatorMeta, TridentCollector collector) {
            long txid = tx.getTransactionId();
            
            long now = System.currentTimeMillis();
            if(now - lastRotate > rotateTime) {
                Map<Long, List<Object>> failed = idsMap.rotate();
                for(Long id: failed.keySet()) {
                    //TODO: this isn't right... it's not in the map anymore
                    fail(id);
                }
                lastRotate = now;
            }
            
            if(idsMap.containsKey(txid)) {
                fail(txid);
            }
            
            _collector.reset(collector);
            if(!prepared) {
                _spout.open(_conf, _context, new SpoutOutputCollector(_collector));
                prepared = true;
            }
            int curMaxBatchSize = _maxBatchSize.get();
            for(int i=0; i<curMaxBatchSize; i++) {
                _spout.nextTuple();
                if(_collector.numEmitted < i) {
                    break;
                }
            }
            
            if (mt != null) {
    			mt.send("EMIT_BATCH", taskId, hashCode(), txid, _collector.ids.size());
    		}
            
            idsMap.put(txid, _collector.ids);
        }

        @Override
        public void success(TransactionAttempt tx) {
            ack(tx.getTransactionId());
        }

        private void ack(long batchId) {
            List<Object> ids = (List<Object>) idsMap.remove(batchId);
            if(ids!=null) {
            	if (mt != null) {
        			mt.send("ACK_BATCH", taskId, hashCode(), batchId, ids.size());
        		}
            	
                for(Object id: ids) {
                    _spout.ack(id);
                }
            }
        }
        
        private void fail(long batchId) {
            List<Object> ids = (List<Object>) idsMap.remove(batchId);
            if(ids!=null) {
            	if (mt != null) {
        			mt.send("FAIL_BATCH", taskId, hashCode(), batchId, ids.size());
        		}
            	
                for(Object id: ids) {
                    _spout.fail(id);
                }
            }
        }
        
        @Override
        public void close() {
        }
        
    }
    
    class RichSpoutCoordinator implements ITridentSpout.BatchCoordinator {
        @Override
        public Object initializeTransaction(long txid, Object prevMetadata, Object currMetadata) {
            return null;
        }

        @Override
        public void success(long txid) {
        }

        @Override
        public boolean isReady(long txid) {
            return true;
        }

        @Override
        public void close() {
        }        
    }
    
    public static class CaptureCollector implements ISpoutOutputCollector {

        TridentCollector _collector;
        public List<Object> ids;
        public int numEmitted;
        
        public void reset(TridentCollector c) {
            _collector = c;
            ids = new ArrayList<Object>();
            numEmitted = 0;
        }
        
        @Override
        public void reportError(Throwable t) {
            _collector.reportError(t);
        }

        @Override
        public List<Integer> emit(String stream, List<Object> values, Object id) {
            if(id!=null) ids.add(id);
            numEmitted++;            
            _collector.emit(values);
            return null;
        }

        @Override
        public void emitDirect(int task, String stream, List<Object> values, Object id) {
            throw new UnsupportedOperationException("Trident does not support direct streams");
        }

		public void reset(ICollector pipelineExecutor) {
			// TODO Auto-generated method stub with exception
			throw new RuntimeException("Not implemented");
		}
        
    }

}
