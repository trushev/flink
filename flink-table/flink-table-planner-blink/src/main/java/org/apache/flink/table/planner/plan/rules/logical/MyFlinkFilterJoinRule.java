package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.FilterJoinRule;

public class MyFlinkFilterJoinRule extends FilterJoinRule.FilterIntoJoinRule {
	public static final MyFlinkFilterJoinRule INSTANCE = new MyFlinkFilterJoinRule();

	private MyFlinkFilterJoinRule() {
		super(true, RelFactories.LOGICAL_BUILDER, FilterJoinRule.TRUE_PREDICATE);
	}

	@Override
	public void onMatch(final RelOptRuleCall call) {
		final Filter filter = call.rel(0);
		final Join join = call.rel(1);
		perform(call, filter, join);
	}
}
