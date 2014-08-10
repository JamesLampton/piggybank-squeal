package org.apache.pig.backend.storm.plans;

import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;

public class SOpPlanVisitor extends PlanVisitor<StormOper, SOperPlan> {

	protected SOpPlanVisitor(SOperPlan plan,
			PlanWalker<StormOper, SOperPlan> walker) {
		super(plan, walker);
	}

	public void visitSOp(StormOper op) throws VisitorException {
		// Do nothing
	}
}
