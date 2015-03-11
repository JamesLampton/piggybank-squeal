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

import java.util.List;

import org.apache.hadoop.io.MapWritable;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.util.Pair;

public abstract class MapIdxWritable<T> extends MapWritable implements IPigIdxState<T> {

	public abstract List<Pair<List<NullableTuple>, List<NullableTuple>>> getTupleBatches(T last);
	
	public String toString() {
		return this.getClass().getSimpleName() + "@" + this.hashCode() + ": " + this.entrySet();
	}
}
