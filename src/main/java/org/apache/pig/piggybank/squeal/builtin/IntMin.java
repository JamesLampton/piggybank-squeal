/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.piggybank.squeal.builtin;

import org.apache.pig.piggybank.squeal.builtin.KMIN.BaseMinInitInverse;
import org.apache.pig.piggybank.squeal.builtin.KMIN.MinIntermed;

/**
 * Replace MIN with a function that will keep k (defaults to 10) intermediate values
 * for buffering negative values.
 * 
 * @author JamesLampton
 *
 */
public class IntMin extends MinMaxDoppleganger<org.apache.pig.builtin.IntMin, Integer> {

//  funcList.add(new FuncSpec(IntMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.BIGDECIMAL)));
	static public class DIntMinInitInverse extends MinMaxDoppleganger.DopInitialInverse<IntMin> { }
	static public class DIntMinInit extends MinMaxDoppleganger.DopInitial<IntMin> {
		public String getInitialInverse() {
			return BaseMinInitInverse.class.getName();
		}
	}
	static public class DIntMinFinal extends MinMaxDoppleganger.DopFinal<IntMin, Integer> {}

	public IntMin() {
		super();
	}

	@Override
	public String getInitial() {
		return DIntMinInit.class.getName();
	}

	@Override
	public String getIntermed() {
		return MinIntermed.class.getName();
	}

	@Override
	public String getFinal() {
		return DIntMinFinal.class.getName();
	}
}
