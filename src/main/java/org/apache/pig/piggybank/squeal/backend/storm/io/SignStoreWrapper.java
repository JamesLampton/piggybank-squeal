package org.apache.pig.backend.storm.io;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;

public class SignStoreWrapper implements StoreFuncInterface, ISignStore {

	protected String storeClass;
	protected StoreFuncInterface wrapped;
	protected AtomicInteger sign;
	protected TupleFactory mTupleFactory = TupleFactory.getInstance();

	public SignStoreWrapper(String[] args) {
		storeClass = args[0];
		
		// Instantiate the wrapped class.
		try {
			Class<?> cls = PigContext.getClassLoader().loadClass(storeClass);
			if (args.length > 1) {
				args = Arrays.copyOfRange(args, 1, args.length);
				Class<?> cls_arr[] = new Class<?>[args.length];
				Arrays.fill(cls_arr, String.class);
				Constructor<?> constr = cls.getConstructor(cls_arr);
				this.wrapped = (StoreFuncInterface) constr.newInstance(args);
			} else {
				this.wrapped = (StoreFuncInterface) cls.newInstance();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public List<String> getUDFs() {
		ArrayList<String> ret = new ArrayList<String>();
		ret.add(storeClass);
		return ret;
	}
	
	@Override
	public String relToAbsPathForStoreLocation(String location, Path curDir)
			throws IOException {
		return wrapped.relToAbsPathForStoreLocation(location, curDir);
	}

	@Override
	public OutputFormat getOutputFormat() throws IOException {
		return wrapped.getOutputFormat();
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		wrapped.setStoreLocation(location, job);
	}

	@Override
	public void checkSchema(ResourceSchema s) throws IOException {
		// FIXME: We need to append the sign to the schema.
		wrapped.checkSchema(s);
	}

	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		wrapped.prepareToWrite(writer);
	}

	public void setSign(AtomicInteger sign) {
		this.sign = sign;
	}
	
	@Override
	public void putNext(Tuple t) throws IOException {
		// Here is where we actually make some modifications.
		// We're going to add a sign field to the end of the schema.
		ArrayList<Object> mProtoTuple = new ArrayList<Object>(t.getAll());
		mProtoTuple.add(sign.get());
		wrapped.putNext(mTupleFactory.newTupleNoCopy(mProtoTuple));
	}

	@Override
	public void setStoreFuncUDFContextSignature(String signature) {
		wrapped.setStoreFuncUDFContextSignature(signature);
	}

	@Override
	public void cleanupOnFailure(String location, Job job) throws IOException {
		wrapped.cleanupOnFailure(location, job);
	}

	@Override
	public void cleanupOnSuccess(String location, Job job) throws IOException {
		wrapped.cleanupOnSuccess(location, job);
	}

}
