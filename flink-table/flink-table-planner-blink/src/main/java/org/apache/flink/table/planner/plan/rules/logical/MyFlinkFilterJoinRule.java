package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.List;

/**
 * My rule.
 */
public class MyFlinkFilterJoinRule extends FilterJoinRule.FilterIntoJoinRule {
	public static final MyFlinkFilterJoinRule INSTANCE = new MyFlinkFilterJoinRule();

	private MyFlinkFilterJoinRule() {
		super(Config.DEFAULT);
	}

	@Override
	public void onMatch(final RelOptRuleCall call) {
		Filter filter = call.rel(0);
		final Join join = call.rel(1);
		if (canOptimize(join, filter)) {
			filter = optimizeFilter(join, filter);
		}
		perform(call, filter, join);
	}

	private Filter optimizeFilter(final Join join, final Filter filter) {
		final List<RexNode> filterOperands = ((RexCall) filter.getCondition()).getOperands();
		RexNode f1 = filterOperands.get(0);
		RexNode f2 = filterOperands.get(1);
		if (f1.isA(SqlKind.LITERAL)) {
			final RexNode tmp = f1;
			f1 = f2;
			f2 = tmp;
		}
		final List<RexNode> joinOperands = ((RexCall) join.getCondition()).getOperands();
		RexNode j1 = joinOperands.get(0);
		if (j1.equals(f1)) {
			j1 = filterOperands.get(1);
		}

		final RexBuilder builder = filter.getCluster().getRexBuilder();
		final SqlOperator operator = ((RexCall) filter.getCondition()).getOperator();
		final RexNode secondFilter = builder.makeCall(operator, j1, f2);
		final RexNode newFilterCondition = builder.makeCall(
				SqlStdOperatorTable.AND,
				filter.getCondition(),
				secondFilter
		);
		return LogicalFilter.create(
				filter.getInput(),
				newFilterCondition,
				com.google.common.collect.ImmutableSet.copyOf(filter.getVariablesSet())
		);
	}

	private boolean canOptimize(final Join join, final Filter filter) {
		if (!canOptimize(join)) {
			return false;
		}
		if (!canOptimize(filter)) {
			return false;
		}
		final List<RexNode> joinOperands = ((RexCall) join.getCondition()).getOperands();
		final List<RexNode> filterOperands = ((RexCall) filter.getCondition()).getOperands();
		return joinOperands.contains(filterOperands.get(0)) || joinOperands.contains(filterOperands.get(1));
	}

	private boolean canOptimize(final Join join) {
		if (join.getJoinType() != JoinRelType.INNER) {
			return false;
		}
		final RexCall condition = (RexCall) join.getCondition();
		if (!condition.isA(SqlKind.EQUALS)) {
			return false;
		}
		final List<RexNode> joinOperands = condition.getOperands();
		if (joinOperands.size() != 2) {
			// never happens
			return false;
		}
		final RexNode o1 = joinOperands.get(0);
		final RexNode o2 = joinOperands.get(1);
		return o1.isA(SqlKind.INPUT_REF) && o2.isA(SqlKind.INPUT_REF);
	}

	private boolean canOptimize(final Filter filter) {
		final RexCall condition = (RexCall) filter.getCondition();
		if (!condition.isA(SqlKind.NOT_EQUALS)) {
			return false;
		}
		final List<RexNode> operands = condition.getOperands();
		if (operands.size() != 2) {
			// never happens
			return false;
		}
		final RexNode f1 = operands.get(0);
		final RexNode f2 = operands.get(1);
		if (f1.isA(SqlKind.INPUT_REF) && f2.isA(SqlKind.LITERAL)) {
			return true;
		} else {
			return f1.isA(SqlKind.LITERAL) && f2.isA(SqlKind.INPUT_REF);
		}
	}
}
