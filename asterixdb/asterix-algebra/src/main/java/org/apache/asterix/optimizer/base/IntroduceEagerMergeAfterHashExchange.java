package org.apache.asterix.optimizer.base;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EagerMergeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.EagerMergePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.HashPartitionExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;

public class IntroduceEagerMergeAfterHashExchange
        implements org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {

        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (!context.getPhysicalOptimizationConfig().getInteractiveMode())
            return false;
        List<LogicalVariable> localgroupByVarList = getLocalGroupByVarList(opRef);
        List<LogicalVariable> globalgroupByVarList = getGlobalGroupByVarList(opRef);

        return rewriteHashPartitionExchangeRecursively(context, opRef, localgroupByVarList, globalgroupByVarList);
    }

    private List<LogicalVariable> getGlobalGroupByVarList(Mutable<ILogicalOperator> opRef) {
        if (opRef == null || opRef.getValue() == null)
            return null;

        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() == LogicalOperatorTag.GROUP) {
            return ((GroupByOperator) op).getGroupByVarList();
        }

        for (Mutable<ILogicalOperator> childRef : op.getInputs()) {
            List<LogicalVariable> result = getGlobalGroupByVarList(childRef);
            if (result != null) {
                return result;
            }
        }

        return null;
    }

    private List<LogicalVariable> getLocalGroupByVarList(Mutable<ILogicalOperator> opRef) {
        return getLocalGroupByVarList(opRef, /* seenFirstGby= */ false);
    }

    private List<LogicalVariable> getLocalGroupByVarList(Mutable<ILogicalOperator> opRef, boolean seenFirstGby) {
        ILogicalOperator op = opRef.getValue();

        if (op.getOperatorTag() == LogicalOperatorTag.GROUP) {
            GroupByOperator groupBy = (GroupByOperator) op;

            if (seenFirstGby) {
                // This is the second GroupBy seen — the local one
                return groupBy.getGroupByVarList();
            } else {
                // First GroupBy seen — this is the global one, keep going
                seenFirstGby = true;
            }
        }

        for (Mutable<ILogicalOperator> childRef : op.getInputs()) {
            if (childRef != null) {
                List<LogicalVariable> result = getLocalGroupByVarList(childRef, seenFirstGby);
                if (result != null) {
                    return result;
                }
            }
        }

        return null;
    }

    private boolean rewriteHashPartitionExchangeRecursively(IOptimizationContext context,
            Mutable<ILogicalOperator> opRef, List<LogicalVariable> localgroupByVarList,
            List<LogicalVariable> globalgroupByVarList) throws AlgebricksException {

        if (opRef == null || opRef.getValue() == null) {
            return false;
        }
        ILogicalOperator op = opRef.getValue();
        boolean rewritten = false;

        for (int i = 0; i < op.getInputs().size(); i++) {
            Mutable<ILogicalOperator> childRef = op.getInputs().get(i);
            ILogicalOperator child = childRef.getValue();

            if (child.getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
                ExchangeOperator exchange = (ExchangeOperator) child;
                if (exchange.getPhysicalOperator() instanceof HashPartitionExchangePOperator) {

                    ILogicalOperator exchangeInput = exchange.getInputs().get(0).getValue();
                    List<LogicalVariable> mergeVars;

                    if (exchangeInput.getOperatorTag() == LogicalOperatorTag.GROUP) {
                        // Exchange is feeding a local group-by → use GLOBAL grouping keys
                        mergeVars = globalgroupByVarList;
                    } else {
                        // No group-by after exchange → use LOCAL keys
                        mergeVars = localgroupByVarList;
                    }

                    // ➕ Insert EagerMergeOperator before the Exchange
                    EagerMergeOperator eagerMerge = new EagerMergeOperator(new ArrayList<>(mergeVars));
                    eagerMerge.getInputs().add(new MutableObject<>(child)); // EagerMerge takes Exchange as input
                    eagerMerge.recomputeSchema();

                    eagerMerge.setPhysicalOperator(new EagerMergePOperator(mergeVars));
                    context.computeAndSetTypeEnvironmentForOperator(eagerMerge);

                    ExchangeOperator oneToOne = new ExchangeOperator();
                    oneToOne.setPhysicalOperator(new OneToOneExchangePOperator());
                    oneToOne.getInputs().add(new MutableObject<>(eagerMerge));
                    oneToOne.recomputeSchema();
                    context.computeAndSetTypeEnvironmentForOperator(oneToOne);

                    // Replace exchange input with EagerMerge
                    op.getInputs().set(i, new MutableObject<>(oneToOne));
                    context.computeAndSetTypeEnvironmentForOperator(op);

                    rewritten = true;
                }
            }

            if (rewriteHashPartitionExchangeRecursively(context, childRef, localgroupByVarList, globalgroupByVarList)) {
                rewritten = true;
            }
        }

        return rewritten;

    }
}
