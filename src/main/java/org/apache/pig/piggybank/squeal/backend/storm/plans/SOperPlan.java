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

package org.apache.pig.piggybank.squeal.backend.storm.plans;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;


import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;

public class SOperPlan extends OperatorPlan<StormOper> {
	
	public Set<String> UDFs = new HashSet<String>();
	public Map<POLoad, StormOper> PLSpoutLink = new HashMap<POLoad, StormOper>();
	private Map<String, StormOper> rootMap;
	private Map<FileSpec, FileSpec> rFileMap;
	private MROperPlan staticPlan;
	
	/* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        SPrinter printer = new SPrinter(ps, this, null);
        printer.setVerbose(true);
        try {
            printer.visit();
        } catch (VisitorException e) {
            // TODO Auto-generated catch block
            throw new RuntimeException("Unable to get String representation of plan:" + e );
        }
        return baos.toString();
    }

	public void addPLSpoutLink(StormOper spout, POLoad pl) {
		// Note, this doesn't survive a clone right now...
		PLSpoutLink.put(pl, spout);
	}
	
	public StormOper getPLSpoutLink(POLoad pl) {
		return PLSpoutLink.get(pl);
	}

	public void setRootMap(Map<String, StormOper> rootMap) {
		this.rootMap = rootMap;
	}
	
	public StormOper getInputSOP(POLoad pl) {
		if (PLSpoutLink.get(pl) != null) {
			return PLSpoutLink.get(pl);
		} else {
			return rootMap.get(pl.getLFile().getFileName());
		}
	}

	public void setReplFileMap(Map<FileSpec, FileSpec> replFileMap) {
		this.rFileMap = replFileMap;
	}
	
	public Map<FileSpec, FileSpec> getReplFileMap() {
		return rFileMap;
	}

	public void setStaticPlan(MROperPlan plan) {
		staticPlan = plan;
	}
	
	public MROperPlan getStaticPlan() {
		return staticPlan;
	}
}
