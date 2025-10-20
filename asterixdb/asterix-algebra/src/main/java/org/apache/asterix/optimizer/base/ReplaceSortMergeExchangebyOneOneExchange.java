package org.apache.asterix.optimizer.base;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.SortMergeExchangePOperator;

public class ReplaceSortMergeExchangebyOneOneExchange implements org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule {
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {

        return false;
    }
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        if (!context.getPhysicalOptimizationConfig().getInteractiveMode()) {
            return false;
        }
        ILogicalOperator op = opRef.getValue();
        boolean changed = false;

        AbstractLogicalOperator aop = (AbstractLogicalOperator) op;


// 1) Must be a logical EXCHANGE
        if (aop.getOperatorTag() != LogicalOperatorTag.EXCHANGE) {
            return false;
        }

        ExchangeOperator exch = (ExchangeOperator) aop;

// 2) Must have a physical op already
        if (exch.getPhysicalOperator() == null) {
            return false;
        }

// 3) Must specifically be a SortMergeExchange physical op
        if (!(exch.getPhysicalOperator() instanceof SortMergeExchangePOperator)) {
            return false;
        }

// 4) Replace with OneToOneExchange
        exch.setPhysicalOperator(new OneToOneExchangePOperator());
        return true;

    }
}
