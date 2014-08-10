package org.apache.pig.backend.storm.plans;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.storm.io.ISignStore;
import org.apache.pig.backend.storm.io.NOPLoad;
import org.apache.pig.backend.storm.io.SignStoreWrapper;
import org.apache.pig.backend.storm.io.SpoutWrapper;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;

public class MRtoSConverter extends MROpPlanVisitor {

	private MROperPlan plan;
	private SOperPlan splan;
	
	// TODO: Track these by the logical operators they represent.
	private Map<String, StormOper> rootMap = new HashMap<String, StormOper>();;
	private Map<String, List<StormOper>> missingRoots = new HashMap<String, List<StormOper>>();
	NodeIdGenerator nig = NodeIdGenerator.getGenerator();
	private String scope;
	private PigContext pc;
	
	public MRtoSConverter(MROperPlan plan, PigContext pc) {
		super(plan, new DependencyOrderWalker<MapReduceOper, MROperPlan>(plan));
		this.plan = plan;
		this.splan = new SOperPlan();
		this.pc = pc;
		scope = plan.getRoots().get(0).getOperatorKey().getScope();
	}
	
	private StormOper getSOp(StormOper.OpType t, String alias){
        return new StormOper(new OperatorKey(scope,nig.getNextNodeId(scope)), t, alias);
    }
	
	void setupStore(List<PhysicalOperator> list, StormOper sop) {
		for (PhysicalOperator op : list) {
			if (op.getClass().isAssignableFrom(POStore.class)) {
				POStore ps = (POStore) op;
				String fn = ps.getSFile().getFileName();
				if (missingRoots.containsKey(fn)) {
					for (StormOper waiting : missingRoots.remove(fn)) {
						try {
							splan.connect(sop, waiting);
						} catch (PlanException e) {
							e.printStackTrace();
						}
					}
				}
				rootMap.put(fn, sop);
			}
		}
	}
	
	static public String getAlias(PhysicalPlan p, boolean useRoots) {
//		System.out.println(useRoots + " Roots: " + p.getRoots() + " Leaves: " + p.getLeaves());
		if (useRoots) {
			PhysicalOperator root = p.getRoots().get(0);
			// TODO: Need to determine if this still works.
			if (root instanceof POPackage) {
				root = p.getSuccessors(root).get(0);
			}
			return (root == null) ? null : root.getAlias();
		}
		
		PhysicalOperator leaf = p.getLeaves().get(0);
		String alias = leaf.getAlias();
		if (leaf instanceof POStore) {
			leaf = p.getPredecessors(leaf).get(0);
		} else if (leaf instanceof POUnion) {
			leaf = ((POUnion) leaf).getInputs().get(0);
		}
		return (leaf == null || leaf.getAlias() == null) ? alias : leaf.getAlias();
	}
	
	public void updateUDFs(PhysicalPlan plan) {		
		try {
			for (POStore store : PlanHelper.getPhysicalOperators(plan, POStore.class)) {
				if (store.getStoreFunc() instanceof ISignStore) {
					ISignStore sf = (ISignStore) store.getStoreFunc();
					splan.UDFs.addAll(sf.getUDFs());
				}
			}
		
			for (POLoad load : PlanHelper.getPhysicalOperators(plan, POLoad.class)) {
				if (load.getLoadFunc() instanceof SpoutWrapper) {
					SpoutWrapper lf = (SpoutWrapper) load.getLoadFunc();
					// Add the spout's UDF so it gets picked up by the Jar.
					splan.UDFs.add(lf.getSpoutClass());
				}
			}
		} catch (VisitorException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void visitMROp(MapReduceOper mr) throws VisitorException {
		splan.UDFs.addAll(mr.UDFs);
		updateUDFs(mr.mapPlan);
		
		new PhyPlanSetter(mr.mapPlan).visit();
        new PhyPlanSetter(mr.reducePlan).visit();
        
		// Map SOP -- Attach to Spout or to Reduce SOP -- replace LOADs
		// Optional Spout Point (May hook onto the end of a Reduce SOP)
		StormOper mo = getSOp(StormOper.OpType.MAP, getAlias(mr.mapPlan, false));
		mo.mapKeyType = mr.mapKeyType;
		splan.add(mo);
		
		// Determine if we need to shuffle before this operator.
		String mapAlias = getAlias(mr.mapPlan, true);
		System.out.println("Checking " + mapAlias + " for shuffle or parallelism constraints...");
		if (pc.getProperties().getProperty(mapAlias + "_shuffleBefore", "false").equalsIgnoreCase("true")) {
			mo.shuffleBefore(true);
		}
		
		// Also, pull a parallelism.
		if (pc.getProperties().getProperty(mapAlias + "_parallel", null) != null) {
			mo.setParallelismHint(Integer.parseInt(pc.getProperties().getProperty(mapAlias + "_parallel")));
		}

		// Look for the input
		for (PhysicalOperator po : mr.mapPlan.getRoots()) {
			POLoad pl = (POLoad)po;
			
			if (pl instanceof NOPLoad) {
				// Skip the static stuff.
				continue;
			}
			
//			System.err.println(po);
			String fn = pl.getLFile().getFileName();
			
			// If it's a Spout, add a new StormOper for it.
			if (!rootMap.containsKey(fn) && pl.getLoadFunc() instanceof SpoutWrapper) {
				// Create a new StormOper for this spout.
				StormOper spout = getSOp(StormOper.OpType.SPOUT, po.getAlias());
				SpoutWrapper sw = ((SpoutWrapper)pl.getLoadFunc());
				spout.setParallelismHint(sw.getParallelismHint());
				spout.setSpout(sw.getSpout());
				spout.setTupleConverter(sw.getTupleConverter());
				splan.add(spout);
				splan.addPLSpoutLink(spout, pl);
				rootMap.put(fn, spout);
			}
			
			if (rootMap.containsKey(fn)) {
				// Add the MapPlan as a dependency on the Stream.
				try {
					splan.connect(rootMap.get(fn), mo);
				} catch (PlanException e) {
					e.printStackTrace();
				}
			} else {
				List<StormOper> wait = missingRoots.get(po);
				if (wait == null) {
					wait = new ArrayList<StormOper>();
					missingRoots.put(fn, wait);
				}
				// Add this map to the waiting list for the Stream
				wait.add(mo);
			}
		}
		
		// Map: We have out input squared away, lets create the foreach function.
		// TODO -- Replace loads
		mo.setPlan(mr.mapPlan);
		if (mr.combinePlan.size() > 0) {
			mo.isCombined = true;
		}
		
		// If this is a Map-only task, we need to find the store and create the link for the output.
		if (mr.reducePlan.size() == 0) {
			setupStore(mr.mapPlan.getLeaves(), mo); // Can a map have leaves if in a MR job?
			return;
		}
		
		// Persist SOP
		StormOper po;
		// See if we're a window.
		String red_alias;
		if (mr.reducePlan.getRoots().get(0) instanceof POPackage) {
			red_alias = getAlias(mr.mapPlan, false);
		} else {
			red_alias = (mr.combinePlan.size() > 0) ? getAlias(mr.combinePlan, false) : getAlias(mr.reducePlan, true);
		}
		
		String window_opts = StormOper.getWindowOpts(pc, red_alias); 
//		System.out.println("RED_ALIAS: " + red_alias + " window_opts: " + window_opts);
		if (mr.combinePlan.size() == 0 || window_opts != null) {
			// Basic reduce or windowed group operator.
			po = getSOp(StormOper.OpType.BASIC_PERSIST, red_alias);
			po.setWindowOptions(window_opts);
		} else {
			// Combine SOP
			po = getSOp(StormOper.OpType.COMBINE_PERSIST, getAlias(mr.combinePlan, false));
			po.setPlan(mr.combinePlan);
			po.mapKeyType = mr.mapKeyType;
		}
		if (mr.getRequestedParallelism() > 0) {
			po.setParallelismHint(mr.getRequestedParallelism());
		}
		splan.add(po);
		
		// Update the UDF list to include those used in persistence.
		// FIXME: This doesn't account for UDFs behind a MultiState.
		if (!po.getStateFactory(pc).getClass().getName().startsWith("storm.trident.testing")) {
			splan.UDFs.add(po.getStateFactory(pc).getClass().getName());
		}
		
		try {
			splan.connect(mo, po);
		} catch (PlanException e) {
			e.printStackTrace();
		}
		
		// Reduce SOP
		StormOper rdo = getSOp(StormOper.OpType.REDUCE_DELTA, getAlias(mr.reducePlan, false));
		splan.add(rdo);
		try {
			splan.connect(po, rdo);
		} catch (PlanException e) {
			e.printStackTrace();
		}
		// TODO -- Remove stores.
		rdo.setPlan(mr.reducePlan);
		// Set window options to tell TriReduce which getTuples routine to call.
		rdo.setWindowOptions(window_opts);
		
//		System.err.println("Reduce leaves: " + mr.reducePlan.getLeaves());
		setupStore(mr.reducePlan.getLeaves(), rdo);
	}
	
	public void convert() {
		// Start walking.
		try {
			// Pull out any static subtrees from the execution plan.
			StaticPlanFixer spf = new StaticPlanFixer(plan, pc);
			spf.convert();
			// Pull out the replicated join creation plan.
			ReplJoinFixer rjf = new ReplJoinFixer(plan, spf.getStaticPlan(), pc);
			rjf.convert();
			if (rjf.getReplFileMap().size() > 0) {
				splan.setReplFileMap(rjf.getReplFileMap());
			}
			if (spf.getStaticPlan().size() > 0) {
				splan.setStaticPlan(spf.getStaticPlan());
			}
			
			visit();
			splan.setRootMap(rootMap);
			
//			System.out.println("ReplFiles: " + splan.replFiles);
			if (missingRoots.size() > 0) {
				// We have some paths that aren't attached to the plan.
				System.out.println("Missing roots: " + missingRoots);
			}
		} catch (VisitorException e) {
			e.printStackTrace();
		}
//		System.out.println("here");
	}

	public SOperPlan getSPlan() {
		return splan;
	}
}
