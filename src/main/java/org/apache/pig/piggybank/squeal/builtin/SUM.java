/*
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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.piggybank.squeal.AlgebraicInverse;

/**
 * Helper function for {@link org.apache.pig.builtin.SUM} that implements
 * {@link org.apache.pig.piggybank.squeal.AlgebraicInverse}.
 */
public class SUM extends org.apache.pig.builtin.SUM {

    public SUM() {
       super();
    }

    static public class InitialInverse extends org.apache.pig.builtin.SUM.Initial {
    	@Override
        public Tuple exec(Tuple input) throws IOException {
    		Tuple t = super.exec(input);
    		if (t.get(0) != null) {
    			t.set(0, negate(t.get(0)));	
    		}
    		
    		return t;
    	}

		private Object negate(Object o) throws ExecException {
			byte dt = DataType.findType(o);
			Object neg_o = null;
			
			switch(dt) {
			case DataType.BIGDECIMAL:
				neg_o = ((BigDecimal)o).negate();
				break;
			case DataType.BIGINTEGER:
				neg_o = ((BigInteger)o).negate();
				break;
			case DataType.FLOAT:
				// Fall through.
			case DataType.DOUBLE:
				neg_o = new Double(-((Number)o).doubleValue());
				break;
			case DataType.INTEGER:
				// Fall through.
			case DataType.LONG:
				neg_o = new Long(-((Number)o).longValue());
				break;
			default:
				int errCode = 2106;
                throw new ExecException("Unknown data type for object: " + o.getClass().getName(), errCode, PigException.BUG);
			}
			
			return neg_o;
		}
    	
    }
    
    public static class Intermediate extends org.apache.pig.builtin.SUM.Intermediate implements AlgebraicInverse  {
    	public String getInitialInverse() {
        	return SUM.InitialInverse.class.getName();
        }
    }
}
