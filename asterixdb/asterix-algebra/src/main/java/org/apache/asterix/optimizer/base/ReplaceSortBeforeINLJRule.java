package org.apache.asterix.optimizer.base;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;

public class ReplaceSortBeforeINLJRule
        implements org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule {
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {

        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (!context.getPhysicalOptimizationConfig().getInteractiveMode()) {
            return false;
        }
        return removeSortsBeforeINLJ(opRef, context);
    }

    private boolean removeSortsBeforeINLJ(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        boolean modified = false;

        ILogicalOperator op = opRef.getValue();

        // Recurse on inputs first
        for (Mutable<ILogicalOperator> childRef : op.getInputs()) {
            modified |= removeSortsBeforeINLJ(childRef, context);
        }

        // If this is an UnnestMap (i.e., an index nested-loop join), check for the Order pattern
        if (op.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
            UnnestMapOperator unnestMap = (UnnestMapOperator) op;
            Mutable<ILogicalOperator> exchangeRef = unnestMap.getInputs().get(0);
            ILogicalOperator exchangeOp = exchangeRef.getValue();

            if (exchangeOp.getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
                ExchangeOperator exchange = (ExchangeOperator) exchangeOp;
                Mutable<ILogicalOperator> orderRef = exchange.getInputs().get(0);
                ILogicalOperator orderOp = orderRef.getValue();

                if (orderOp.getOperatorTag() == LogicalOperatorTag.ORDER) {
                    OrderOperator order = (OrderOperator) orderOp;

                    Mutable<ILogicalOperator> secondExchangeRef = order.getInputs().get(0);
                    ILogicalOperator secondExchangeOp = secondExchangeRef.getValue();

                    if (secondExchangeOp.getOperatorTag() == LogicalOperatorTag.EXCHANGE
                            && ((ExchangeOperator) secondExchangeOp).getPhysicalOperator()
                                    .getOperatorTag() == PhysicalOperatorTag.HASH_PARTITION_EXCHANGE) {

                        // Replace chain: UnnestMap <- Exchange <- Order <- Exchange
                        // With:        UnnestMap <- Exchange (second one)
                        unnestMap.getInputs().set(0, secondExchangeRef);
                        recomputePropsAndTypes(op, context);
                        modified = true;
                    }
                }
            }
        }

        return modified;
    }

    private void recomputePropsAndTypes(ILogicalOperator op, IOptimizationContext context) throws AlgebricksException {
        for (Mutable<ILogicalOperator> inputRef : op.getInputs()) {
            recomputePropsAndTypes(inputRef.getValue(), context);
        }
        context.computeAndSetTypeEnvironmentForOperator(op);
        op.computeDeliveredPhysicalProperties(context);
    }

}
