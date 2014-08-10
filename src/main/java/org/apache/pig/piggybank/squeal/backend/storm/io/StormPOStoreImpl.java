package org.apache.pig.backend.storm.io;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputFormat;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStoreImpl;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;

public class StormPOStoreImpl extends POStoreImpl {

	private int partitionIndex;
	private RecordWriter writer;
	private TaskAttemptContext context;
	private AtomicInteger sign;

	public StormPOStoreImpl(String stormId, int partitionIndex, AtomicInteger sign) {
		this.partitionIndex = partitionIndex;
		this.sign = sign;
		
		// "storm.id" "PigStorm-3-0-1-1363457130" PigStorm-3-0-1-1363536122
		// TaskAttemptID(String jtIdentifier, int jobId, boolean isMap, int taskId, int id) 
		TaskAttemptID attemptID = new TaskAttemptID(stormId, (int)(System.currentTimeMillis()/1000), true, partitionIndex, sign.get());
		
		// Create a fake TaskContext for this stuff.
		Configuration outputConf = new Configuration();
		this.context = HadoopShims.createTaskAttemptContext(outputConf, attemptID);
	}

	@Override
	public StoreFuncInterface createStoreFunc(POStore store) throws IOException {
		StoreFuncInterface storeFunc = store.getStoreFunc();

        // call the setStoreLocation on the storeFunc giving it the
        // Job. Typically this will result in the OutputFormat of the
        // storeFunc storing the output location in the Configuration
        // in the Job. The PigOutFormat.setLocation() method will merge 
        // this modified Configuration into the configuration of the
        // Context we have
        PigOutputFormat.setLocation(context, store);
        OutputFormat outputFormat = storeFunc.getOutputFormat();

        // create a new record writer
        try {
            writer = outputFormat.getRecordWriter(context);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
 
        storeFunc.prepareToWrite(writer);
        
        if (storeFunc instanceof ISignStore) {
        	((ISignStore)storeFunc).setSign(sign);
        }
        
        return storeFunc;
	}
	
	@Override
    public void tearDown() throws IOException {
        if (writer != null) {
            try {
                writer.close(context);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            writer = null;
        }
    }
	
	@Override
    public void cleanUp() throws IOException {
        if (writer != null) {
            try {
                writer.close(context);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            writer = null;
        }
    }
}
