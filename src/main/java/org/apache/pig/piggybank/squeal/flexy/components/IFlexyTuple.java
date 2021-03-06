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

package org.apache.pig.piggybank.squeal.flexy.components;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.pig.piggybank.squeal.backend.storm.state.MapIdxWritable;
import org.apache.pig.piggybank.squeal.flexy.model.FFields;

import backtype.storm.tuple.Fields;

public interface IFlexyTuple extends Collection {

	Object get(int i);
	int getInteger(int i);
	int size();
	List<Object> getValues();
	Object getValueByField(String fName);
	byte[] getBinary(int i);
	long getLong(int i);
	boolean getBoolean(int i);
	List<Object> select(FFields gr_fields);

}
