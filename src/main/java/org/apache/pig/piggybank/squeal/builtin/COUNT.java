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
import org.apache.pig.piggybank.squeal.AlgebraicInverse;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * Helper function for {@link org.apache.pig.builtin.COUNT} that implements
 * {@link org.apache.pig.piggybank.squeal.AlgebraicInverse}.
 */
public class COUNT extends org.apache.pig.builtin.COUNT {
    private static TupleFactory mTupleFactory = TupleFactory.getInstance();

    public String getInitial() {
        return Initial.class.getName();
    }
	
    static public class Initial extends org.apache.pig.builtin.COUNT.Initial implements AlgebraicInverse {
        
    	@Override
    	public String getInitialInverse() {
    		return InitialInverse.class.getName();
    	}        
    }
    
    static public class InitialInverse extends Initial {

        @Override
        public Tuple exec(Tuple input) throws IOException {
        	Tuple res = super.exec(input);
            return mTupleFactory.newTuple(-(Long)res.get(0));
        }
    }
}
