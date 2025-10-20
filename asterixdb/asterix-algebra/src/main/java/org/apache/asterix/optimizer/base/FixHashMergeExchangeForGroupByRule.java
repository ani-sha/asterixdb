package org.apache.asterix.optimizer.base;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.HashPartitionMergeExchangePOperator;
//import org.apache.hyracks.algebricks.core.algebra.operators.physical.PartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class FixHashMergeExchangeForGroupByRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(org.apache.commons.lang3.mutable.Mutable<ILogicalOperator> opRef,
                              IOptimizationContext ctx) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.GROUP) {
            return false;
        }
        GroupByOperator gby = (GroupByOperator) op;

        if (gby.getInputs().isEmpty()) {
            return false;
        }

        ILogicalOperator input = gby.getInputs().get(0).getValue();
        if (input.getOperatorTag() != LogicalOperatorTag.EXCHANGE) {
            return false;
        }

        ExchangeOperator exch = (ExchangeOperator) input;
        if (!(exch.getPhysicalOperator() instanceof HashPartitionMergeExchangePOperator)) {
            return false;
        }

        HashPartitionMergeExchangePOperator hashMerge =
                (HashPartitionMergeExchangePOperator) exch.getPhysicalOperator();

        // Collect group-by variables
        List<LogicalVariable> gvars = gby.getGroupByVarList();
//        gby.getGroupByVarList()
//        gby.getGroupByVarList().forEach(p -> gvars.add(p.first));

        // Build ascending order for all group-by vars
        List<OrderColumn> newMergeOrder = new ArrayList<>();
        for (LogicalVariable v : gvars) {
            newMergeOrder.add(new OrderColumn(v, IOrder.OrderKind.ASC));
        }

        // Replace the merge keys (hash keys remain as-is)
        exch.setPhysicalOperator(new HashPartitionMergeExchangePOperator(
                newMergeOrder, hashMerge.getPartitionFields(), hashMerge.getDomain(), hashMerge.getPartitionsMap()));

        ctx.computeAndSetTypeEnvironmentForOperator(exch);
        exch.computeDeliveredPhysicalProperties(ctx);

        // Recompute props/types
        ctx.computeAndSetTypeEnvironmentForOperator(gby);
        gby.computeDeliveredPhysicalProperties(ctx);

        return true;
    }

    @Override
    public boolean rewritePost(org.apache.commons.lang3.mutable.Mutable<ILogicalOperator> opRef,
                               IOptimizationContext ctx) {
        return false;
    }
}
