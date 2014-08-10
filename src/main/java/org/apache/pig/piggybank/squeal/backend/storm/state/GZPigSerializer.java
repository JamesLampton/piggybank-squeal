package org.apache.pig.backend.storm.state;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.io.AbstractMapWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.NullableBag;
import org.apache.pig.impl.io.NullableBooleanWritable;
import org.apache.pig.impl.io.NullableBytesWritable;
import org.apache.pig.impl.io.NullableDoubleWritable;
import org.apache.pig.impl.io.NullableFloatWritable;
import org.apache.pig.impl.io.NullableIntWritable;
import org.apache.pig.impl.io.NullableLongWritable;
import org.apache.pig.impl.io.NullableText;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;

import backtype.storm.tuple.Values;

import storm.trident.state.Serializer;

public class GZPigSerializer implements Serializer {
	PigSerializer ps = new PigSerializer();
	
	@Override
	public byte[] serialize(Object obj) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			GZIPOutputStream gz = new GZIPOutputStream(baos);
			gz.write(ps.serialize(obj));
			gz.close();
			
			return baos.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Object deserialize(byte[] b) {
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(b);
			GZIPInputStream gz = new GZIPInputStream(bais);
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			
			byte[] buf = new byte[4096];
			while (true) {
				int r = gz.read(buf);
				if (r == -1) {
					break;
				}
				baos.write(buf, 0, r);
			}
			gz.close();
			
			Object o = ps.deserialize(baos.toByteArray());
			return o;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	
}
