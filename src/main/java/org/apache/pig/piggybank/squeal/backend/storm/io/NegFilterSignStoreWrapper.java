package org.apache.pig.backend.storm.io;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
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

public class NegFilterSignStoreWrapper extends SignStoreWrapper {

	public NegFilterSignStoreWrapper(String[] args) {
		super(args);
	}
	
	@Override
	public void putNext(Tuple t) throws IOException {
		if (sign.get() > 0) {
			wrapped.putNext(t);
		}
	}
}
