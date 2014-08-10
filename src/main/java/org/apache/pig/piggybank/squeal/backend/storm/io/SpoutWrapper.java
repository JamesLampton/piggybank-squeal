package org.apache.pig.backend.storm.io;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.Expression;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.storm.oper.TriMakePigTuples;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;
import org.mortbay.util.ajax.JSON;

import storm.trident.operation.BaseFunction;
import backtype.storm.generated.StreamInfo;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsGetter;

public class SpoutWrapper extends LoadFunc implements LoadMetadata, LoadCaster {

	private String spoutClass;
	private String jsonArgs;
	private String parallelismHint;

	public int getParallelismHint() {
		return (parallelismHint == null) ? 0 : Integer.parseInt(parallelismHint);
	}
	
	public SpoutWrapper() {
		this(null, null, null);
	}
	
	public SpoutWrapper(String spoutClass) {
		this(spoutClass, null);
	}
	
	public SpoutWrapper(String spoutClass, String jsonArgs) {
		this(spoutClass, jsonArgs, null);
	}
	
	public SpoutWrapper(String spoutClass, String jsonArgs, String parallelismHint) {
//		System.out.println("SpoutWrapper created: " + spoutClass + " " + jsonArgs);
		this.spoutClass = spoutClass;
		this.jsonArgs = jsonArgs;
		this.parallelismHint = parallelismHint;
	}
	
	public String getSpoutClass() {
		return spoutClass;
	}
	
	public IRichSpout getSpout() {
		try {
			Class<?> cls = PigContext.getClassLoader().loadClass(spoutClass);
			if (jsonArgs != null && jsonArgs.length() > 0) {
				Object o[] = (Object[]) JSON.parse(jsonArgs);
				Class<?> cls_arr[] = new Class<?>[o.length];
				for (int i = 0; i < o.length; i++) {
					cls_arr[i] = o[i].getClass();
				}
				Constructor<?> constr = cls.getConstructor(cls_arr);
				return (IRichSpout) constr.newInstance(o);					
			} else {
				return (IRichSpout) cls.newInstance();
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void setLocation(String location, Job job) throws IOException {
		// TODO Auto-generated method stub
		
	}

	static public class EmptyInputFormat extends InputFormat {

		@Override
		public RecordReader createRecordReader(InputSplit arg0,
				TaskAttemptContext arg1) throws IOException,
				InterruptedException {
			// This should never be called.
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public List getSplits(JobContext arg0) throws IOException,
				InterruptedException {
			return new ArrayList();
		}
	}
	
	@Override
	public InputFormat getInputFormat() throws IOException {
		return new EmptyInputFormat();
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Tuple getNext() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public ResourceSchema getSchema(String location, Job job)
			throws IOException {
		Schema s = new Schema();
		IRichSpout l = getSpout();
	
		OutputFieldsGetter declarer = new OutputFieldsGetter();
		
		l.declareOutputFields(declarer);
		
		for (Entry<String, StreamInfo> e : declarer.getFieldsDeclaration().entrySet()) {
			for (String field : e.getValue().get_output_fields()) {
				s.add(new Schema.FieldSchema(field, DataType.BYTEARRAY));
			}
		}
		
		return new ResourceSchema(s);
	}

	@Override
	public ResourceStatistics getStatistics(String location, Job job)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getPartitionKeys(String location, Job job)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setPartitionFilter(Expression partitionFilter)
			throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	Utf8StorageConverter conv = new Utf8StorageConverter();
	
	@Override
	public Boolean bytesToBoolean(byte[] b) throws IOException {
		return conv.bytesToBoolean(b);
	}

	@Override
	public Long bytesToLong(byte[] b) throws IOException {
		return conv.bytesToLong(b);
	}

	@Override
	public Float bytesToFloat(byte[] b) throws IOException {
		return conv.bytesToFloat(b);
	}

	@Override
	public Double bytesToDouble(byte[] b) throws IOException {
		return conv.bytesToDouble(b);
	}

	@Override
	public Integer bytesToInteger(byte[] b) throws IOException {
		return conv.bytesToInteger(b);
	}

	@Override
	public String bytesToCharArray(byte[] b) throws IOException {
		return conv.bytesToCharArray(b);
	}

	@Override
	public Map<String, Object> bytesToMap(byte[] b,
			ResourceFieldSchema fieldSchema) throws IOException {
		return conv.bytesToMap(b, fieldSchema);
	}

	@Override
	public Tuple bytesToTuple(byte[] b, ResourceFieldSchema fieldSchema)
			throws IOException {
		return conv.bytesToTuple(b, fieldSchema);
	}

	@Override
	public DataBag bytesToBag(byte[] b, ResourceFieldSchema fieldSchema)
			throws IOException {
		return conv.bytesToBag(b, fieldSchema);
	}
	
	@Override
	public DateTime bytesToDateTime(byte[] b) throws IOException {
		return conv.bytesToDateTime(b);
	}

	@Override
	public BigInteger bytesToBigInteger(byte[] b) throws IOException {
		return conv.bytesToBigInteger(b);
	}

	@Override
	public BigDecimal bytesToBigDecimal(byte[] b) throws IOException {
		return conv.bytesToBigDecimal(b);
	}

	public Class<? extends BaseFunction> getTupleConverter() {
		return TriMakePigTuples.class;
	}


}
