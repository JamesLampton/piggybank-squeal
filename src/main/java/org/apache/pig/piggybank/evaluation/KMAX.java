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

package org.apache.pig.piggybank.evaluation;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.piggybank.evaluation.KOpBase.KMAXBase;
import org.joda.time.DateTime;

public class KMAX extends KMAXBase<DataByteArray> {
	static public class KMAXDataByteArrayFinal extends KMAXFinal<DataByteArray> {	}

	@Override
	public String getFinal() {
		return KMAXDataByteArrayFinal.class.getName();
	}

	// For Double
	static public class KMAXDoubleFinal extends KMAXFinal<Double> {	}
	static public class KMAXDouble extends KMAXBase<Double> {
		@Override
		public String getFinal() {
			return KMAXDoubleFinal.class.getName();
		}	
	}

	// For Float
	static public class KMAXFloatFinal extends KMAXFinal<Float> {	}
	static public class KMAXFloat extends KMAXBase<Float> {
		@Override
		public String getFinal() {
			return KMAXFloatFinal.class.getName();
		}	
	}

	// For Integer
	static public class KMAXIntegerFinal extends KMAXFinal<Integer> {	}
	static public class KMAXInteger extends KMAXBase<Integer> {
		@Override
		public String getFinal() {
			return KMAXIntegerFinal.class.getName();
		}	
	}

	// For Long
	static public class KMAXLongFinal extends KMAXFinal<Long> {	}
	static public class KMAXLong extends KMAXBase<Long> {
		@Override
		public String getFinal() {
			return KMAXLongFinal.class.getName();
		}	
	}

	// For String
	static public class KMAXStringFinal extends KMAXFinal<String> {	}
	static public class KMAXString extends KMAXBase<String> {
		@Override
		public String getFinal() {
			return KMAXStringFinal.class.getName();
		}	
	}

	// For DateTime
	static public class KMAXDateTimeFinal extends KMAXFinal<DateTime> {	}
	static public class KMAXDateTime extends KMAXBase<DateTime> {
		@Override
		public String getFinal() {
			return KMAXDateTimeFinal.class.getName();
		}	
	}

	// For BigDecimal
	static public class KMAXBigDecimalFinal extends KMAXFinal<BigDecimal> {	}
	static public class KMAXBigDecimal extends KMAXBase<BigDecimal> {
		@Override
		public String getFinal() {
			return KMAXBigDecimalFinal.class.getName();
		}	
	}

	// For BigInteger
	static public class KMAXBigIntegerFinal extends KMAXFinal<BigInteger> {	}
	static public class KMAXBigInteger extends KMAXBase<BigInteger> {
		@Override
		public String getFinal() {
			return KMAXBigIntegerFinal.class.getName();
		}	
	}

	/* (non-Javadoc)
	 * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
	 */
	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> funcList = new ArrayList<FuncSpec>();
		funcList.add(new FuncSpec(this.getClass().getName(), Schema.generateNestedSchema(DataType.BAG, DataType.BYTEARRAY)));
		funcList.add(new FuncSpec(KMAXDouble.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.DOUBLE)));
		funcList.add(new FuncSpec(KMAXFloat.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.FLOAT)));
		funcList.add(new FuncSpec(KMAXInteger.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.INTEGER)));
		funcList.add(new FuncSpec(KMAXLong.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.LONG)));
		funcList.add(new FuncSpec(KMAXString.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.CHARARRAY)));
		funcList.add(new FuncSpec(KMAXDateTime.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.DATETIME)));
		funcList.add(new FuncSpec(KMAXBigDecimal.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.BIGDECIMAL)));
		funcList.add(new FuncSpec(KMAXBigInteger.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.BIGINTEGER)));
		return funcList;
	}
}
