package org.apache.asterix.optimizer.base;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.SortedMergeOneExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;

public class ReplaceTopExchangeToMergeExchangeRule
        implements org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule {
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {

        return false;
    }

    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        List<LogicalVariable> globalGroupByProducedVarList = new ArrayList<>();
        getGlobalGroupByProducedVarList(opRef, globalGroupByProducedVarList);
        return rewriteTopExchange(context, opRef, globalGroupByProducedVarList);

    }

    private boolean rewriteTopExchange(IOptimizationContext context, Mutable<ILogicalOperator> opRef,
            List<LogicalVariable> globalGroupByProducedVarList) {
        if (opRef == null || opRef.getValue() == null) {
            return false;
        }
        ILogicalOperator op = opRef.getValue();
        boolean rewritten = false;
        for (int i = 0; i < op.getInputs().size(); i++) {
            Mutable<ILogicalOperator> childRef = op.getInputs().get(i);
            ILogicalOperator child = childRef.getValue();
            if (op.getOperatorTag() == LogicalOperatorTag.EXCHANGE && child.getOperatorTag() == LogicalOperatorTag.GROUP
                    && child.getAnnotations().containsKey("isGlobal")) {
                List<OrderColumn> orderColumns = new ArrayList<>();
                for (LogicalVariable var : globalGroupByProducedVarList) {
                    orderColumns.add(new OrderColumn(var, OrderOperator.IOrder.OrderKind.ASC));
                }
                SortedMergeOneExchangePOperator mergeOp = new SortedMergeOneExchangePOperator(orderColumns);
                ExchangeOperator exchange = (ExchangeOperator) op;
                exchange.setPhysicalOperator(mergeOp);
                exchange.setExecutionMode(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
                rewritten = true;
            }
            if (rewriteTopExchange(context, childRef, globalGroupByProducedVarList)) {
                rewritten = true;
            }

        }
        return rewritten;
    }

    private void getGlobalGroupByProducedVarList(Mutable<ILogicalOperator> opRef,
            List<LogicalVariable> globalGroupByProducedVarList) {
        if (opRef == null || opRef.getValue() == null) {
            return;
        }

        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() == LogicalOperatorTag.GROUP) {
            ((GroupByOperator) op).getProducedVariablesExceptNestedPlans(globalGroupByProducedVarList);
            return; // Stop at first GROUP
        }

        for (Mutable<ILogicalOperator> childRef : op.getInputs()) {
            getGlobalGroupByProducedVarList(childRef, globalGroupByProducedVarList);
        }
    }
}
