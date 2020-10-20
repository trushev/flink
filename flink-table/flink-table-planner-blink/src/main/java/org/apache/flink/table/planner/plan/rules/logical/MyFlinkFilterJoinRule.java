package org.apache.flink.table.planner.plan.rules.logical;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.Iterator;

public class MyFlinkFilterJoinRule extends FilterJoinRule.FilterIntoJoinRule {
	public static final MyFlinkFilterJoinRule INSTANCE = new MyFlinkFilterJoinRule();

	private MyFlinkFilterJoinRule() {
		super(true, RelFactories.LOGICAL_BUILDER, FilterJoinRule.TRUE_PREDICATE);
	}

	@Override
	public void onMatch(final RelOptRuleCall call) {
		final Filter filter = call.rel(0);
		final Join join = call.rel(1);

		final boolean optimize = true;

		final LogicalFilter resultLogical;
		if (optimize) {
			final Iterator<RexNode> it = ((RexCall) filter.getCondition())
					.operands
					.stream()
					.iterator();
			it.next();
			final RexNode value = it.next();
			final RexNode s0 = ((RexCall) join.getCondition()).operands.stream().iterator().next();
			final RexBuilder builder = filter.getCluster().getRexBuilder();
			final RexNode secondFilter = builder.makeCall(SqlStdOperatorTable.NOT_EQUALS, s0, value);
			final RexNode result = builder.makeCall(
					SqlStdOperatorTable.AND,
					filter.getCondition(),
					secondFilter);
			final LogicalFilter logicalFilter = (LogicalFilter) filter;
			resultLogical = LogicalFilter.create(
					logicalFilter.getInput(),
					result,
					ImmutableSet.copyOf(filter.getVariablesSet()));
		} else {
			resultLogical = (LogicalFilter) filter;
		}
		perform(call, resultLogical, join);
	}
}
