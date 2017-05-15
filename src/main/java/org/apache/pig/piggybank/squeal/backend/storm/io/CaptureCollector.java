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

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.piggybank.squeal.flexy.components.ICollector;

import backtype.storm.spout.ISpoutOutputCollector;

public class CaptureCollector implements ISpoutOutputCollector {

    ICollector _collector;
    public List<Object> ids;
    public int numEmitted;
    
    @Override
    public void reportError(Throwable t) {
        _collector.reportError(t);
    }

    @Override
    public List<Integer> emit(String stream, List<Object> values, Object id) {
        if(id!=null) ids.add(id);
        numEmitted++;            
        _collector.emit(values);
        return null;
    }

    @Override
    public void emitDirect(int task, String stream, List<Object> values, Object id) {
        throw new UnsupportedOperationException("Flexy does not support direct streams");
    }

	public void reset(ICollector c) {
		 _collector = c;
		 ids = new ArrayList<Object>();
		 numEmitted = 0;
	}
    
}
