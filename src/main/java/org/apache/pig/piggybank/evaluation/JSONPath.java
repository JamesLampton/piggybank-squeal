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

package org.apache.pig.piggybank.evaluation;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class JSONPath extends EvalFunc<Map> {

	org.apache.pig.piggybank.util.JSONPath jp;
	String[] ids;
	
	@Override
	public Map exec(Tuple t) throws IOException {
		Map<String, String> ret = new HashMap<String, String>();		

//		ret.put("test", "value");
		
		if (jp == null) {
			jp = new org.apache.pig.piggybank.util.JSONPath((String) t.get(0));
			ids = ((String)t.get(0)).split(" ");
		}
		
		try {
			List<Object> proc = jp.process((String) t.get(1));
			for (int i = 0; i < ids.length; i++) {
				Object v = proc.get(i);
				ret.put(ids[i], v == null ? "" : v.toString());
			}
		} catch (Exception e) {
			ret.put("error", "Could not parse: " + t.get(1));
		}
		
		return ret;
	}

	public Schema outputSchema(Schema input) {
		try{
            Schema mapSchema = new Schema();
            mapSchema.add(new Schema.FieldSchema("value", DataType.CHARARRAY));

            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                                                   mapSchema, DataType.MAP));
        }catch (Exception e){
           return null;
        }
	}
}
