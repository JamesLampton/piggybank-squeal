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
public class StringMin extends MinMaxDoppleganger<org.apache.pig.builtin.StringMin, String> {

//  funcList.add(new FuncSpec(StringMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.BIGDECIMAL)));
	static public class DStringMinInitInverse extends MinMaxDoppleganger.DopInitialInverse<StringMin> { }
	static public class DStringMinInit extends MinMaxDoppleganger.DopInitial<StringMin> {
		public String getInitialInverse() {
			return BaseMinInitInverse.class.getName();
		}
	}
	static public class DStringMinFinal extends MinMaxDoppleganger.DopFinal<StringMin, String> {}

	public StringMin() {
		super();
	}

	@Override
	public String getInitial() {
		return DStringMinInit.class.getName();
	}

	@Override
	public String getIntermed() {
		return MinIntermed.class.getName();
	}

	@Override
	public String getFinal() {
		return DStringMinFinal.class.getName();
	}
}
