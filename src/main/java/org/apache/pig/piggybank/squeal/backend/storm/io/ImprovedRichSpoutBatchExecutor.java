package org.apache.pig.piggybank.squeal.backend.storm.io;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.pig.piggybank.squeal.MonkeyPatch;
import org.apache.pig.piggybank.squeal.metrics.IMetricsTransport;
import org.apache.pig.piggybank.squeal.metrics.MetricsTransportFactory;

import backtype.storm.Config;
import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.RotatingMap;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;
import storm.trident.util.TridentUtils;

// Taken mostly from RichSpoutBatchExecutor.
public class ImprovedRichSpoutBatchExecutor implements ITridentSpout {
	public static final String MAX_BATCH_SIZE_CONF = "topology.spout.max.batch.size";

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
    
    class RichSpoutEmitter implements ITridentSpout.Emitter<Object> {
        int _maxBatchSize;
        boolean prepared = false;
        CaptureCollector _collector;
        RotatingMap<Long, List<Object>> idsMap;
        Map _conf;
        TopologyContext _context;
        long lastRotate = System.currentTimeMillis();
        long rotateTime;
		private IMetricsTransport mt;
		private double sample_rate = 0.1;
		private int taskId;
		private int taskIdx;
		private String compId;
		private String stormId;
		private String hostname;
		static public final String SAMPLE_RATE_KEY = "pig.streaming.metrics.sample.rate";

        public RichSpoutEmitter(Map conf, TopologyContext context) {
            _conf = conf;
            _context = context;
            Number batchSize = (Number) conf.get(MAX_BATCH_SIZE_CONF);
            if(batchSize==null) batchSize = 1000;
            _maxBatchSize = batchSize.intValue();
            _collector = new CaptureCollector();
            idsMap = new RotatingMap(3);
            rotateTime = 1000L * ((Number)conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)).intValue();
            
            // Pull a metrics transport if configured.
    		mt = MetricsTransportFactory.getInstance(conf, ClassLoader.getSystemClassLoader());
    		Random r = new Random();
    		
    		Object sr = conf.get(SAMPLE_RATE_KEY);
    		if (sr != null) {
    			sample_rate  = Double.parseDouble(sr.toString());
    		}
    		
    		try {
    			taskId = context.getThisTaskId();
    			taskIdx = context.getThisTaskIndex();
    			compId = context.getThisComponentId();
    			stormId = (String) conf.get("storm.id");
    			hostname = InetAddress.getLocalHost().getHostName();
    		} catch (Exception e) {
    			// Leave things as unknown on error.
    		}
    		
    		// send a declare message.
    		if (mt != null) {
    			mt.send("DECL_EMITTER", hostname, stormId, taskId, taskIdx, compId, hashCode(), _spout.toString());
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
            for(int i=0; i<_maxBatchSize; i++) {
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
    
    static class CaptureCollector implements ISpoutOutputCollector {

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
        
    }

}
