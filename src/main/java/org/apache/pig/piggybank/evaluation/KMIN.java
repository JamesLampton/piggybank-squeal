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
import org.apache.pig.piggybank.evaluation.KOpBase.KMINBase;
import org.joda.time.DateTime;

public class KMIN extends KMINBase<DataByteArray> {

	static public class KMINDataByteArrayFinal extends KMINFinal<DataByteArray> {	}

	@Override
	public String getFinal() {
		return KMINDataByteArrayFinal.class.getName();
	}

	// For Double
	static public class KMINDoubleFinal extends KMINFinal<Double> {	}
	static public class KMINDouble extends KMINBase<Double> {
		@Override
		public String getFinal() {
			return KMINDoubleFinal.class.getName();
		}	
	}

	// For Float
	static public class KMINFloatFinal extends KMINFinal<Float> {	}
	static public class KMINFloat extends KMINBase<Float> {
		@Override
		public String getFinal() {
			return KMINFloatFinal.class.getName();
		}	
	}

	// For Integer
	static public class KMINIntegerFinal extends KMINFinal<Integer> {	}
	static public class KMINInteger extends KMINBase<Integer> {
		@Override
		public String getFinal() {
			return KMINIntegerFinal.class.getName();
		}	
	}

	// For Long
	static public class KMINLongFinal extends KMINFinal<Long> {	}
	static public class KMINLong extends KMINBase<Long> {
		@Override
		public String getFinal() {
			return KMINLongFinal.class.getName();
		}	
	}

	// For String
	static public class KMINStringFinal extends KMINFinal<String> {	}
	static public class KMINString extends KMINBase<String> {
		@Override
		public String getFinal() {
			return KMINStringFinal.class.getName();
		}	
	}

	// For DateTime
	static public class KMINDateTimeFinal extends KMINFinal<DateTime> {	}
	static public class KMINDateTime extends KMINBase<DateTime> {
		@Override
		public String getFinal() {
			return KMINDateTimeFinal.class.getName();
		}	
	}

	// For BigDecimal
	static public class KMINBigDecimalFinal extends KMINFinal<BigDecimal> {	}
	static public class KMINBigDecimal extends KMINBase<BigDecimal> {
		@Override
		public String getFinal() {
			return KMINBigDecimalFinal.class.getName();
		}	
	}

	// For BigInteger
	static public class KMINBigIntegerFinal extends KMINFinal<BigInteger> {	}
	static public class KMINBigInteger extends KMINBase<BigInteger> {
		@Override
		public String getFinal() {
			return KMINBigIntegerFinal.class.getName();
		}	
	}

	/* (non-Javadoc)
	 * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
	 */
	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> funcList = new ArrayList<FuncSpec>();
		funcList.add(new FuncSpec(this.getClass().getName(), Schema.generateNestedSchema(DataType.BAG, DataType.BYTEARRAY)));
		funcList.add(new FuncSpec(KMINDouble.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.DOUBLE)));
		funcList.add(new FuncSpec(KMINFloat.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.FLOAT)));
		funcList.add(new FuncSpec(KMINInteger.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.INTEGER)));
		funcList.add(new FuncSpec(KMINLong.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.LONG)));
		funcList.add(new FuncSpec(KMINString.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.CHARARRAY)));
		funcList.add(new FuncSpec(KMINDateTime.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.DATETIME)));
		funcList.add(new FuncSpec(KMINBigDecimal.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.BIGDECIMAL)));
		funcList.add(new FuncSpec(KMINBigInteger.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.BIGINTEGER)));
		return funcList;
	}

}
