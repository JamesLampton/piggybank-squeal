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

package org.apache.pig.piggybank.squeal.backend.storm.state;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.piggybank.squeal.backend.storm.oper.TriBasicPersist.TriBasicPersistState;
import org.apache.pig.piggybank.squeal.backend.storm.oper.TriWindowCombinePersist.WindowCombineState;

public abstract class MapIdxWritable<T> implements Map<Writable, Writable>, IPigIdxState<T>, Writable {

	public abstract List<Pair<List<NullableTuple>, List<NullableTuple>>> getTupleBatches(T last);

	public String toString() {
		return this.getClass().getSimpleName() + "@" + this.hashCode() + ": " + this.entrySet();
	}

	private Map<Writable, Writable> instance;

	/** Default constructor. */
	public MapIdxWritable() {
		super();
		this.instance = new HashMap<Writable, Writable>();
	}

	/**
	 * Copy constructor.
	 * 
	 * @param other the map to copy from
	 */
	public MapIdxWritable(MapIdxWritable other) {
		this();
		copy(other);
	}

	/** Used by child copy constructors. */
	protected void copy(Writable other) {
		if (other != null) {
			try {
				DataOutputBuffer out = new DataOutputBuffer();
				other.write(out);
				DataInputBuffer in = new DataInputBuffer();
				in.reset(out.getData(), out.getLength());
				readFields(in);
			} catch (IOException e) {
				throw new IllegalArgumentException("map cannot be copied: " +
						e.getMessage());
			}
		} else {
			throw new IllegalArgumentException("source map cannot be null");
		}
	}

	@Override
	public void clear() {
		instance.clear();
	}

	@Override
	public boolean containsKey(Object key) {
		return instance.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return instance.containsValue(value);
	}

	@Override
	public Set<Map.Entry<Writable, Writable>> entrySet() {
		return instance.entrySet();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (obj instanceof MapWritable) {
			Map map = (Map) obj;
			if (size() != map.size()) {
				return false;
			}

			return entrySet().equals(map.entrySet());
		}

		return false;
	}

	@Override
	public Writable get(Object key) {
		return instance.get(key);
	}

	@Override
	public int hashCode() {
		return 1 + this.instance.hashCode();
	}

	@Override
	public boolean isEmpty() {
		return instance.isEmpty();
	}

	@Override
	public Set<Writable> keySet() {
		return instance.keySet();
	}

	@Override
	@SuppressWarnings("unchecked")
	public Writable put(Writable key, Writable value) {
		return instance.put(key, value);
	}

	@Override
	public void putAll(Map<? extends Writable, ? extends Writable> t) {
		for (Map.Entry<? extends Writable, ? extends Writable> e: t.entrySet()) {
			put(e.getKey(), e.getValue());
		}
	}

	@Override
	public Writable remove(Object key) {
		return instance.remove(key);
	}

	@Override
	public int size() {
		return instance.size();
	}

	@Override
	public Collection<Writable> values() {
		return instance.values();
	}

	// Writable

	// Setup a static mapping of well known Pig types.
	static Map<Class, Byte> static_classToIdMap = new HashMap<Class, Byte>();
	static Map<Byte, Class> static_idToClassMap = new HashMap<Byte, Class>();

	static void addToMap(Class klazz, Byte b) {
		static_classToIdMap.put(klazz, b);
		static_idToClassMap.put(b, klazz);
	}

	static {
		addToMap(ArrayWritable.class,
				Byte.valueOf(Integer.valueOf(-127).byteValue())); 
		addToMap(BooleanWritable.class,
				Byte.valueOf(Integer.valueOf(-126).byteValue()));
		addToMap(BytesWritable.class,
				Byte.valueOf(Integer.valueOf(-125).byteValue()));
		addToMap(FloatWritable.class,
				Byte.valueOf(Integer.valueOf(-124).byteValue()));
		addToMap(IntWritable.class,
				Byte.valueOf(Integer.valueOf(-123).byteValue()));
		addToMap(LongWritable.class,
				Byte.valueOf(Integer.valueOf(-122).byteValue()));
		addToMap(MapWritable.class,
				Byte.valueOf(Integer.valueOf(-121).byteValue()));
		addToMap(MD5Hash.class,
				Byte.valueOf(Integer.valueOf(-120).byteValue()));
		addToMap(NullWritable.class,
				Byte.valueOf(Integer.valueOf(-119).byteValue()));
		addToMap(ObjectWritable.class,
				Byte.valueOf(Integer.valueOf(-118).byteValue()));
		addToMap(SortedMapWritable.class,
				Byte.valueOf(Integer.valueOf(-117).byteValue()));
		addToMap(Text.class,
				Byte.valueOf(Integer.valueOf(-116).byteValue()));
		addToMap(TwoDArrayWritable.class,
				Byte.valueOf(Integer.valueOf(-115).byteValue()));

		// UTF8 is deprecated so we don't support it

		addToMap(VIntWritable.class,
				Byte.valueOf(Integer.valueOf(-114).byteValue()));
		addToMap(VLongWritable.class,
				Byte.valueOf(Integer.valueOf(-113).byteValue()));
		
		// Add the Pig-Squeal types.
		addToMap(CombineTupleWritable.class,
				Byte.valueOf(Integer.valueOf(-112).byteValue()));
		addToMap(TriBasicPersistState.class,
				Byte.valueOf(Integer.valueOf(-111).byteValue()));
		addToMap(WindowCombineState.class,
				Byte.valueOf(Integer.valueOf(-110).byteValue()));
	}

	private static Byte getId(Class<? extends Writable> klazz, Map<Class, Byte> classToIdMap, DataOutput out) throws IOException {		
		Byte id = static_classToIdMap.get(klazz);
		if (id != null) {
//			System.err.println("[USED] Static id " + klazz.getName() + " " + id);
			return id;
		}

		// Assign one dynamically
		id = classToIdMap.get(klazz);
		if (id != null) {
//			System.err.println("[USED] Dynamic id " + klazz.getName() + " " + id);
			return id;
		}

		// Add the class to the map and write the mapping.
		id = (byte) (classToIdMap.size() + 1);
		classToIdMap.put(klazz, id);

		out.writeByte(id);
		out.writeUTF(klazz.getName());

//		System.err.println("[ASSIGNED] Dynamic id " + klazz.getName() + " " + id);
		return id;
	}


	@Override
	public void write(DataOutput out) throws IOException {
		Map<Class, Byte> classToIdMap = new HashMap<Class, Byte>();

		// Write out the number of entries in the map
//		System.err.println("Writing " + this + " " + instance.size());
		out.writeInt(instance.size());

		// Then write out each key/value pair
		for (Map.Entry<Writable, Writable> e: instance.entrySet()) {
			out.writeByte(getId(e.getKey().getClass(), classToIdMap, out));
			e.getKey().write(out);
			out.writeByte(getId(e.getValue().getClass(), classToIdMap, out));
			e.getValue().write(out);
//			System.err.println("WROTE: " + e.getKey() + " " + e.getValue());
		}
	}

	private static Class getClass(byte b, Map<Byte, Class> idToClassMap, DataInput in) throws IOException {
		Class klazz = static_idToClassMap.get(b);
		if (klazz != null) {
//			System.err.println("READ: [USED] Static id " + klazz.getName() + " " + b);
			return klazz;
		}

		klazz = idToClassMap.get(b);
		if (klazz != null) {
//			System.err.println("READ: [USED] Dynamic id " + klazz.getName() + " " + b);
			return klazz;
		}
		
		// Read the class name.
		String className = in.readUTF();
		try {
			klazz = Class.forName(className);
		} catch (ClassNotFoundException e) {
			throw new IOException("Class " + className + " not found for id " + b, e);
		}
		idToClassMap.put(b, klazz);
		
		// Read the extra type byte.
		b = in.readByte();
		
//		System.err.println("READ: [ASSIGNED] Dynamic id " + klazz.getName() + " " + b);

		return klazz;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
		Map<Byte, Class> idToClassMap = new HashMap<Byte, Class>();

		// First clear the map.  Otherwise we will just accumulate
		// entries every time this method is called.
		this.instance.clear();

		// Read the number of entries in the map
		int entries = in.readInt();

		// Then read each key/value pair

		for (int i = 0; i < entries; i++) {
			Writable key = (Writable) ReflectionUtils.newInstance(getClass(
					in.readByte(), idToClassMap, in), null);

			key.readFields(in);

			Writable value = (Writable) ReflectionUtils.newInstance(getClass(
					in.readByte(), idToClassMap, in), null);

			value.readFields(in);
			instance.put(key, value);
//			System.err.println("READ: " + key + " " + value);
		}
	}
}
