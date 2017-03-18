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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.pig.piggybank.squeal.flexy.components.ISource;

import backtype.storm.generated.StreamInfo;
import backtype.storm.topology.IComponent;
import backtype.storm.topology.OutputFieldsGetter;
import backtype.storm.tuple.Fields;

public class FFields {

	public FFields(String... fields) {
		// TODO Auto-generated constructor stub with exception
		throw new RuntimeException("Not implemented");
	}

	public FFields(List<String> list) {
		// TODO Auto-generated constructor stub with exception
		throw new RuntimeException("Not implemented");
	}

	public static FFields fieldsConcat(FFields a, FFields b) {
		
		// storm.trident.util.TridentUtils
//		public static Fields fieldsConcat(Fields... fields) {
//	        List<String> ret = new ArrayList<String>();
//	        for(Fields f: fields) {
//	            if(f!=null) ret.addAll(f.toList());
//	        }
//	        return new Fields(ret);
//	    }
		
		// TODO Auto-generated method stub with exception
		throw new RuntimeException("Not implemented");
	}

	public static FFields getSingleOutputStreamFields(ISource source) {
		// TODO Auto-generated method stub with exception
		throw new RuntimeException("Not implemented");
		// storm.trident.util.TridentUtils
//		public static Fields getSingleOutputStreamFields(IComponent component) {
//	        OutputFieldsGetter getter = new OutputFieldsGetter();
//	        component.declareOutputFields(getter);
//	        Map<String, StreamInfo> declaration = getter.getFieldsDeclaration();
//	        if(declaration.size()!=1) {
//	            throw new RuntimeException("Trident only supports components that emit a single stream");
//	        }
//	        StreamInfo si = declaration.values().iterator().next();
//	        if(si.is_direct()) {
//	            throw new RuntimeException("Trident does not support direct streams");
//	        }
//	        return new Fields(si.get_output_fields());        
//	    }
	}

	public String get(int i) {
		// TODO Auto-generated method stub with exception
		throw new RuntimeException("Not implemented");
	}
	
	public List<String> toList() {
		return null;
	}
}
