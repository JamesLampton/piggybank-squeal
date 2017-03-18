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

package org.apache.pig.piggybank.squeal.flexy.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// This is a shadow of the functionality from Storm's Fields and some TridentUtils
//import backtype.storm.tuple.Fields;

public class FFields implements Serializable {
	private List<String> _fields;
    private Map<String, Integer> _index = new HashMap<String, Integer>();

	public FFields(String... fields) {
		this(Arrays.asList(fields));
	}

	public FFields(List<String> fields) {
		_fields = new ArrayList<String>(fields.size());
        for (String field : fields) {
            if (_index.containsKey(field))
                throw new IllegalArgumentException(
                    String.format("duplicate field '%s'", field)
                );
            _index.put(field, _fields.size());
            _fields.add(field);
        }
	}

	public static FFields fieldsConcat(FFields... fields) {
		// Similar to: storm.trident.util.TridentUtils
		List<String> ret = new ArrayList<String>();
		for(FFields f: fields) {
			if(f!=null) ret.addAll(f._fields);
		}
		return new FFields(ret);
	}

	public String get(int i) {
		return _fields.get(i);
	}
	
	public List<String> toList() {
		return new ArrayList<String>(_fields);
	}
}
