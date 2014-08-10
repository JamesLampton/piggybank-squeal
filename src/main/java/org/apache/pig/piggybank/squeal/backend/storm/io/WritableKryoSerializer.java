package org.apache.pig.backend.storm.io;

import org.apache.hadoop.io.Writable;
import org.apache.pig.backend.storm.state.PigSerializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class WritableKryoSerializer extends Serializer<Writable> {
	
	PigSerializer ps = new PigSerializer();

	@Override
	public Writable read(Kryo kryo, Input input, Class<Writable> w) {
		int len = input.readInt();
		byte[] buf = input.readBytes(len);
		return (Writable) ps.deserialize(buf);
	}

	@Override
	public void write(Kryo kryo, Output output, Writable w) {
		byte[] buf = ps.serialize(w);
		output.writeInt(buf.length);
		output.write(buf);
	}

}
