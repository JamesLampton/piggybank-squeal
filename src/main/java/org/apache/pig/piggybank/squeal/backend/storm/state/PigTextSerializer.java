package org.apache.pig.backend.storm.state;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.builtin.PigStreaming;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.util.StorageUtil;

import backtype.storm.tuple.Values;

import storm.trident.state.Serializer;

/*
 * Good for key serialization for States, but don't use to store the value because
 * this mechanism looses the schema information!
 */
public class PigTextSerializer implements Serializer<List<Object>> {
	
	private PigStreaming ps;
	private TupleFactory tf;

	public PigTextSerializer() {
		ps = new PigStreaming();
		tf = TupleFactory.getInstance();
	}
	
	@Override
	public byte[] serialize(List<Object> objl) {
		try {
			ArrayList<Object> arr = new ArrayList<Object>(objl.size());
			for (Object o : objl) {
				arr.add(((PigNullableWritable) o).getValueAsPigType());
			}
			return ps.serialize(tf.newTupleNoCopy(arr));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<Object> deserialize(byte[] b) {
		try {
			Values v = new Values();			
			for (Object o : ps.deserialize(b).getAll()) {
				v.add(HDataType.getWritableComparableTypes(((Tuple)o).get(0), DataType.findType(o)));
			}
			return v;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}		
	}

	Object writeReplace() throws ObjectStreamException {
		return new SerializedForm();
	}
	
	static class SerializedForm implements Serializable {
		Object readResolve() throws ObjectStreamException {
			return new PigTextSerializer();
		}
	}
}
