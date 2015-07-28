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

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Replace MIN with a function that will keep k (defaults to 10) intermediate values
 * for buffering negative values.
 * 
 * @author JamesLampton
 *
 */
public class KMIN extends MinMaxDoppleganger<org.apache.pig.builtin.MIN> {
	
	static final int DEFAULTK = 10;
	static final boolean REVERSE = false; 

	// Just need one of these.
	static public class MinIntermed extends MinMaxDoppleganger.DopIntermed {
		public MinIntermed() {
			super(REVERSE, DEFAULTK);
		}
	}
	
	// Need a group of these for each type.
	// BaseMin
	static public class BaseMinInitInverse extends MinMaxDoppleganger.DopInitialInverse<org.apache.pig.builtin.MIN> { }
	static public class BaseMinInit extends MinMaxDoppleganger.DopInitial<org.apache.pig.builtin.MIN> {
		public String getInitialInverse() {
			return BaseMinInitInverse.class.getName();
		}
	}
	static public class BaseMinFinal extends MinMaxDoppleganger.DopFinal<org.apache.pig.builtin.MIN> {}
	public KMIN() {
		super(BaseMinInit.class.getName(), MinIntermed.class.getName(), BaseMinFinal.class.getName());
	}
	
	/* (non-Javadoc)
     * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
     */
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(), Schema.generateNestedSchema(DataType.BAG, DataType.BYTEARRAY)));
        funcList.add(new FuncSpec(DoubleMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.DOUBLE)));
        funcList.add(new FuncSpec(FloatMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.FLOAT)));
        funcList.add(new FuncSpec(IntMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.INTEGER)));
        funcList.add(new FuncSpec(LongMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.LONG)));
        funcList.add(new FuncSpec(StringMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.CHARARRAY)));
        funcList.add(new FuncSpec(DateTimeMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.DATETIME)));
        funcList.add(new FuncSpec(BigDecimalMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.BIGDECIMAL)));
        funcList.add(new FuncSpec(BigIntegerMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.BIGINTEGER)));
        return funcList;
    }
}
