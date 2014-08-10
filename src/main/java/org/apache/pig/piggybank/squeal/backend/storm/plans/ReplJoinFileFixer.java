package org.apache.pig.backend.storm.plans;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PhyPlanSetter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.storm.io.SpoutWrapper;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.ReverseDependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;

import storm.trident.TridentTopology;

/**
 * The purpose of this class is to find elements of a MapReduce plan that
 * contribute to replicated joins.  For our purposes, they will be rooted
 * in regular load functions and terminate in stores that are FRJoin files.
 * These chains need to be removed and executed before the Storm job.
 * 
 * @author jhl1
 *
 */
public class ReplJoinFileFixer extends SOpPlanVisitor {

	private SOperPlan plan;
	Map<FileSpec, FileSpec> rFileMap = new HashMap<FileSpec, FileSpec>();
	
	public ReplJoinFileFixer(SOperPlan plan) {
		super(plan, new DependencyOrderWalker<StormOper, SOperPlan>(plan));
		this.plan = plan;
		this.rFileMap = plan.getReplFileMap();
	}
		
	class FRJoinFileReplacer extends PhyPlanVisitor {
		
		public FRJoinFileReplacer(PhysicalPlan plan) {
			super(plan, new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(plan));
		}
		
	    @Override
	    public void visitFRJoin(POFRJoin join) throws VisitorException {
	    	List<FileSpec> newrepl = new ArrayList<FileSpec>();
	    	
	    	// Extract the files.
	    	for (FileSpec f : join.getReplFiles()) {
	    		if (f == null) {
	    			newrepl.add(f);
	    			continue;
	    		}
	    		
	    		// Use the rFileMap to swap things.
	    		newrepl.add(rFileMap.get(f));
	    	}
	    	
	    	join.setReplFiles(newrepl.toArray(join.getReplFiles()));
	    }
	}
	
	public void visitSOp(StormOper sop) throws VisitorException {
		if (sop.plan == null) {
			return;
		}
		
		new FRJoinFileReplacer(sop.plan).visit();
	}
	
	public void convert() {		
		// Start walking.
		try {
			visit();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
