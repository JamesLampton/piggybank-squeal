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

package org.apache.pig.piggybank.squeal.backend.storm.io;

import java.io.IOException;

import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;

public class DebugOutput extends PigStorage {
	
	private boolean verbose;

	public DebugOutput() {
		this.verbose = true;
	}
	
	public DebugOutput(String verb) {
		this.verbose = verb.equalsIgnoreCase("true") || verb.equalsIgnoreCase("1");
	}
	
	@Override
	public void putNext(Tuple t) throws IOException {
		if (verbose) {
			System.out.println("DEBUG: " + t);
		}
	}
}
