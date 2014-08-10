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

/**
 * The purpose of this class is to find elements of a MapReduce plan that
 * contribute to replicated joins.  For our purposes, they will be rooted
 * in regular load functions and terminate in stores that are FRJoin files.
 * These chains need to be removed and executed before the Storm job.
 * 
 * @author jhl1
 *
 */
public class ReplJoinFixer extends MROpPlanVisitor {

	private MROperPlan plan;
	private MROperPlan staticPlan = new MROperPlan();
	Map<String, MapReduceOper> fnToMOP = new HashMap<String, MapReduceOper>();
	Map<FileSpec, FileSpec> rFileMap = new HashMap<FileSpec, FileSpec>();
	boolean swapFiles = false;
	String pathFixerConst = null;

//	private Set<FileSpec> replFiles = new HashSet<FileSpec>();
	
	public ReplJoinFixer(MROperPlan plan, MROperPlan staticPlan, PigContext pc) {
		super(plan, new DependencyOrderWalker<MapReduceOper, MROperPlan>(plan));
		this.plan = plan;
		this.staticPlan = staticPlan;
		pathFixerConst = pc.getProperties().getProperty("pig.streaming.fixedtmp");
	}
	
	class FRJoinFinder extends PhyPlanVisitor {

		public FRJoinFinder(PhysicalPlan plan) {
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
	    		
	    		// The File/basename etc works for hadoop paths, so we're going to do it the sloppy way.
	    		String[] parts = f.getFileName().split("/");
	    		int parent_index = parts.length - 2;
	    		if (pathFixerConst != null) {
	    			parts[parent_index] = pathFixerConst;
	    		} else {
	    			parts[parent_index] += "-persist";
	    		}
	    		parts[parent_index+1] = Integer.toString(rFileMap.size());
	    		String newfn = StringUtils.join(parts, "/");
	    		
	    		FileSpec newspec = new FileSpec(newfn, f.getFuncSpec());
	    		rFileMap.put(f, newspec);
	    		newrepl.add(newspec);
	    		
//	    		System.out.println("Join: " + join + " " + f);
//	    		replFiles.add(f);
	    	}
	    }
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
		
	public void visitMROp(MapReduceOper mr) throws VisitorException {
		if (swapFiles) {
			new FRJoinFileReplacer(mr.mapPlan).visit();
	        new FRJoinFileReplacer(mr.reducePlan).visit();
			return;
		}
		
		// Cycle through the leaves adding them to the fnToMOP.
		List<PhysicalOperator> leaves = new ArrayList<PhysicalOperator>(mr.mapPlan.size() + mr.reducePlan.size());
		leaves.addAll(mr.mapPlan.getLeaves());
		leaves.addAll(mr.reducePlan.getLeaves());
		for (PhysicalOperator po : leaves) {
			if (po instanceof POStore) {
				String fn = ((POStore)po).getSFile().getFileName();
//				System.out.println("OP: " + mr.getOperatorKey() + " file: " + fn);
				fnToMOP.put(fn, mr);
			}
		}

		// Collect the replicated files, we'll sweep back along fnToMOP later.
        new FRJoinFinder(mr.mapPlan).visit();
        new FRJoinFinder(mr.reducePlan).visit();        
	}
	
	public void convert() {
		convert(null);
	}
	
	public void convert(Map<FileSpec, FileSpec> rFileMap) {
		if (rFileMap == null) {
			swapFiles = false;
		} else {
			this.rFileMap = rFileMap;
			swapFiles = true;
		}
		
		// Start walking.
		try {
			visit();
			extractReplPlans();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void extractReplPlans() throws PlanException {
//		System.out.println("rFiles: " + rFiles);
		for (FileSpec f : rFileMap.keySet()) {			
			// Determine the leaf of the plan that produces this file.
			// and change the POStore to non-temp.
			MapReduceOper mr = fnToMOP.get(f.getFileName());
			List<PhysicalOperator> leaves = new ArrayList<PhysicalOperator>(mr.mapPlan.size() + mr.reducePlan.size());
			leaves.addAll(mr.mapPlan.getLeaves());
			leaves.addAll(mr.reducePlan.getLeaves());
			for (PhysicalOperator po : leaves) {
				if (po instanceof POStore) {
					((POStore) po).setIsTmpStore(false);
				}
			}
			
			// Move operator to replPlan.
			moveToReplPlan(mr);
		}
	}

	private void moveToReplPlan(MapReduceOper mr_cur) throws PlanException {
		// This op was probably moved by the StaticPlanFixer.
		if (mr_cur == null) {
			return;
		}
		
		// We're going to do this recursively.
		List<MapReduceOper> preds = plan.getPredecessors(mr_cur);
				
		// Remove the current operator.
		plan.remove(mr_cur);
		// Put it into the new plan.
		staticPlan.add(mr_cur);
		
		if (preds == null) {
			return;
		}
		
		for (MapReduceOper pred : preds) {
			// Move all the predecessors.
			moveToReplPlan(pred);
			// And link in the new plan.
			staticPlan.connect(pred, mr_cur);
		}
	}

	public Map<FileSpec, FileSpec> getReplFileMap() {
		return rFileMap;
	}
}
