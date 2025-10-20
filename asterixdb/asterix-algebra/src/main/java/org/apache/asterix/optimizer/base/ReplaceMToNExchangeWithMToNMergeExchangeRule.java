package org.apache.asterix.optimizer.base;

import java.util.*;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.*;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.*;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.*;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;

public class ReplaceMToNExchangeWithMToNMergeExchangeRule
        implements org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (!(opRef.getValue() instanceof DistributeResultOperator))
            return false;
        if (!context.getPhysicalOptimizationConfig().getInteractiveMode())
            return false;

        LogicalVariable initial = getInitialGroupingVar(opRef);
        if (initial == null)
            return false;

        return rewriteFromGroupingVar(opRef, initial, initial);
    }

    private LogicalVariable getInitialGroupingVar(Mutable<ILogicalOperator> opRef) {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator assign = (AssignOperator) op;
            if (!assign.getVariables().isEmpty()) {
                return assign.getVariables().get(0); // Start with $$n_name
            }
        }
        for (Mutable<ILogicalOperator> input : op.getInputs()) {
            LogicalVariable v = getInitialGroupingVar(input);
            if (v != null)
                return v;
        }
        return null;
    }

    private boolean rewriteFromGroupingVar(Mutable<ILogicalOperator> opRef, LogicalVariable groupVar, LogicalVariable initialGroupVar) throws AlgebricksException {
        if (opRef == null || opRef.getValue() == null) return false;
        boolean rewritten = false;
        ILogicalOperator op = opRef.getValue();

        // If group by: update the groupVar
        if (op.getOperatorTag() == LogicalOperatorTag.GROUP) {
            GroupByOperator gby = (GroupByOperator) op;
            groupVar = gby.getGroupByVarList().getFirst();

        }

        // If UNIONALL: remap groupVar based on third position to left input
        if (op.getOperatorTag() == LogicalOperatorTag.UNIONALL) {
            UnionAllOperator union = (UnionAllOperator) op;
            for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> mapping : union.getVariableMappings()) {
                if (mapping.third.equals(groupVar)) {
                    groupVar = mapping.first;
                    break;
                }
            }
            boolean left = rewriteFromGroupingVar(union.getInputs().get(0), groupVar, initialGroupVar);
            boolean right = rewriteFromGroupingVar(union.getInputs().get(1), groupVar, initialGroupVar);
            return left || right;
        }

        // Recurse
        for (Mutable<ILogicalOperator> input : op.getInputs()) {
            rewritten |= rewriteFromGroupingVar(input, groupVar, initialGroupVar);
        }

        // If exchange: apply merge key
        if (op.getOperatorTag() == LogicalOperatorTag.EXCHANGE && !(op.getInputs().get(0).getValue().getOperatorTag() == LogicalOperatorTag.UNNEST)){
            ExchangeOperator exch = (ExchangeOperator) op;
            if (exch.getPhysicalOperator() instanceof HashPartitionExchangePOperator hashOp) {
                if (op.getInputs().get(0).getValue().getOperatorTag() == LogicalOperatorTag.GROUP) {
                    GroupByOperator gby = (GroupByOperator) op.getInputs().get(0).getValue();
                    List<OrderColumn> orderColumns = new ArrayList<>();
                    for (LogicalVariable v : gby.getVariables()) {
                        orderColumns.add(new OrderColumn(v, OrderOperator.IOrder.OrderKind.ASC));
                    }
                    exch.setPhysicalOperator(new HashPartitionMergeExchangePOperator(
                            orderColumns, hashOp.getHashFields(), hashOp.getDomain(), hashOp.getPartitionsMap()));
                }
                else if(op.getInputs().get(0).getValue().getOperatorTag() != LogicalOperatorTag.GROUP)  {
                    exch.setPhysicalOperator(new HashPartitionMergeExchangePOperator(
                            List.of(new OrderColumn(groupVar, OrderOperator.IOrder.OrderKind.ASC)),
                            hashOp.getHashFields(), hashOp.getDomain(), hashOp.getPartitionsMap()));
                }
                return true;
            }
            if (exch.getPhysicalOperator() instanceof BroadcastExchangePOperator bcastOp) {
                exch.setPhysicalOperator(new BroadcastMergeExchangePOperator(
                        bcastOp.getDomain(), List.of(new OrderColumn(groupVar, OrderOperator.IOrder.OrderKind.ASC))));
                return true;
            }
        }

        return rewritten;
    }
}
