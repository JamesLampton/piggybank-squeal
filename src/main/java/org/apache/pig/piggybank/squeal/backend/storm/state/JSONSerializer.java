package org.apache.pig.backend.storm.state;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.AbstractMapWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.data.DataBag;
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
import org.mortbay.util.ajax.JSON;

import backtype.storm.tuple.Values;

import storm.trident.state.Serializer;

public class JSONSerializer implements Serializer {
	
	Object convertToJSONSafe(Object o) {
		if (o instanceof Tuple) {
			Tuple t = (Tuple) o;
			
			List ls = t.getAll();
			for (int i = 0; i < ls.size(); i++) {
				ls.set(i, convertToJSONSafe(ls.get(i)));
			}
			return ls;
		} else if (o instanceof DataBag) {
			DataBag b = (DataBag) o;
			
			List<Object> ret = new ArrayList<Object>();
			for (Object ent : b) {
				ret.add(convertToJSONSafe(ent));
			}
			return ret;
		}
		
		// Hope for the best!
		return o;
	}
	
	
	@Override
	public byte[] serialize(Object o) {
		
		try {
			if (o instanceof Values) {
				Values objl = (Values) o;
				
				// Get the pig type.
				List<Object> jsonMe = new ArrayList<Object>();
				
				// First, write the type.
				for (int i = 0; i < objl.size(); i++) {
					PigNullableWritable pnw = (PigNullableWritable) objl.get(i);
					
					jsonMe.add(this.convertToJSONSafe(pnw.getValueAsPigType()));
				}
				return JSON.toString(jsonMe).getBytes();
			} else {
				throw new RuntimeException("Unexpected type: " + o.getClass());
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Object deserialize(byte[] b) {
		throw new RuntimeException("Not Implemented");
	}

}
