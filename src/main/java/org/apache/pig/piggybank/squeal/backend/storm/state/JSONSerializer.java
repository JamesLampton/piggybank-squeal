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

import java.util.ArrayList;
import java.util.List;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.piggybank.squeal.flexy.components.ISerializer;
import org.mortbay.util.ajax.JSON;

import backtype.storm.tuple.Values;

public class JSONSerializer implements ISerializer {
	
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
