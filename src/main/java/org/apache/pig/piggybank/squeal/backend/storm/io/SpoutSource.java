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

package org.apache.pig.piggybank.squeal.backend.storm.io;

import java.io.Serializable;
import java.util.Map;

import org.apache.pig.piggybank.squeal.flexy.components.IRunContext;
import org.apache.pig.piggybank.squeal.flexy.components.ISource;
import org.apache.pig.piggybank.squeal.flexy.components.SourceOutputCollector;
import org.apache.pig.piggybank.squeal.flexy.model.FFields;

import backtype.storm.generated.StreamInfo;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.IComponent;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsGetter;

public class SpoutSource implements ISource, Serializable {
	private IRichSpout s;
	public SpoutSource(IRichSpout s) {
		this.s = s;
	}

	@Override
	public IComponent getSpout() {
		return s;
	}

	@Override
	public void fail(Object msgId) {
		s.fail(msgId);
	}

	@Override
	public void ack(Object msgId) {
		s.ack(msgId);
	}

	@Override
	public void open(IRunContext context,
			SourceOutputCollector sourceOutputCollector) {
		s.open(context.getStormConf(), context.getStormTopologyContext(), new SpoutOutputCollector(sourceOutputCollector.getOutputCollector()));
	}

	@Override
	public void nextTuple() {
		s.nextTuple();
	}

	@Override
	public FFields getOutputFields() {
		// storm.trident.util.TridentUtils
		//			public static Fields getSingleOutputStreamFields(IComponent component) {
		OutputFieldsGetter getter = new OutputFieldsGetter();
		s.declareOutputFields(getter);
		Map<String, StreamInfo> declaration = getter.getFieldsDeclaration();
		if(declaration.size()!=1) {
			throw new RuntimeException("Flexy only supports components that emit a single stream");
		}
		StreamInfo si = declaration.values().iterator().next();
		if(si.is_direct()) {
			throw new RuntimeException("Flexy does not support direct streams");
		}
		return new FFields(si.get_output_fields());
	}
}
