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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.piggybank.squeal.MonkeyPatch;

public class NOPLoad extends POLoad {

	static public class NOPRecordReader extends RecordReader {
		@Override
		public void close() throws IOException {}

		@Override
		public Object getCurrentKey() throws IOException, InterruptedException {
			return null;
		}

		@Override
		public Object getCurrentValue() throws IOException,
				InterruptedException {
			return null;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1)
				throws IOException, InterruptedException {
			
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return false;
		}
		
	}
	
	static public class NOPInputFormat extends InputFormat {
		@Override
		public RecordReader createRecordReader(InputSplit arg0,
				TaskAttemptContext arg1) throws IOException,
				InterruptedException {
			return new NOPRecordReader();
		}

		@Override
		public List getSplits(JobContext arg0) throws IOException,
				InterruptedException {
			return new ArrayList();
		}
		
	}
	
	static public class NOPLoader extends LoadFunc {
		@Override
		public void setLocation(String location, Job job) throws IOException {

		}

		@Override
		public InputFormat getInputFormat() throws IOException {
			return new NOPInputFormat();
		}

		@Override
		public void prepareToRead(RecordReader reader, PigSplit split)
				throws IOException {
			
		}

		@Override
		public Tuple getNext() throws IOException {
			return null;
		}		
	}
	
	public NOPLoad(OperatorKey k, POLoad load) {
		super(k);
		
		FileSpec newLFile = new FileSpec(load.getLFile().getFileName(), 
				new FuncSpec(NOPLoader.class.getName() + "()"));
		
		this.setLFile(newLFile);
		this.addOriginalLocation(load.getAlias(), load.getOriginalLocations());
	}

	@Override
    public Result getNextTuple() throws ExecException {
		Result res = new Result();
		res.returnStatus = POStatus.STATUS_EOP;
		return res;
	}
	
	@Override
    public String name() {
        return (getLFile() != null) ? getAliasString() + "NOPLoad" + "(" + getLFile().toString()
                + ")" + " - " + mKey.toString() : getAliasString() + "Load" + "("
                + "DummyFil:DummyLdr" + ")" + " - " + mKey.toString();
    }
}
