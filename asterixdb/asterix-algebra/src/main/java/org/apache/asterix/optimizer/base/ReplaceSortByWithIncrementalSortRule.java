package org.apache.asterix.optimizer.base;

import java.util.List;

import org.apache.asterix.optimizer.rules.am.BTreeJobGenParams;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.*;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.IncrementalSortPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractStableSortPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;

/**
 * Rewrites a global order feeding a BTree search (inner branch of an
 * index-nested-loop-join) into an incremental sort when possible.
 */
public class ReplaceSortByWithIncrementalSortRule
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
        return traverse(opRef, context);
    }

    private boolean traverse(Mutable<ILogicalOperator> opRef, IOptimizationContext ctx) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        boolean modified = false;
        for (Mutable<ILogicalOperator> inputRef : op.getInputs()) {
            modified |= traverse(inputRef, ctx);
        }

        // look for BTree search (inner of INLJ)
        if (op.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP
                && op.getPhysicalOperator().getOperatorTag() == PhysicalOperatorTag.BTREE_SEARCH) {
            modified |= tryRewrite((AbstractUnnestMapOperator) op, ctx);
        }
        return modified;
    }

    /**
     * Attempts to replace a preceding StableSortPOperator by an IncrementalSortPOperator
     * if the sort keys match the probe keys of the BTree search.
     */
    private boolean tryRewrite(AbstractUnnestMapOperator unnest, IOptimizationContext ctx) throws AlgebricksException {
        Mutable<ILogicalOperator> childRef = unnest.getInputs().get(0);
        AbstractLogicalOperator child = (AbstractLogicalOperator) childRef.getValue();
        OrderOperator orderOp = null;

        // the order might be wrapped with an exchange
        if (child.getOperatorTag() == LogicalOperatorTag.ORDER) {
            orderOp = (OrderOperator) child;
        } else if (child.getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
            ExchangeOperator exch = (ExchangeOperator) child;
            ILogicalOperator exchInput = exch.getInputs().get(0).getValue();
            if (exchInput.getOperatorTag() == LogicalOperatorTag.ORDER) {
                orderOp = (OrderOperator) exchInput;
            }
        }

        if (orderOp == null) {
            return false;
        }

        // only replace stable sort
        if (orderOp.getPhysicalOperator().getOperatorTag() != PhysicalOperatorTag.STABLE_SORT) {
            return false;
        }

        AbstractStableSortPOperator sortPhys = (AbstractStableSortPOperator) orderOp.getPhysicalOperator();
        OrderColumn[] sortColumns = sortPhys.getSortColumns();

        // read probe keys from the BTree function arguments
        AbstractFunctionCallExpression funcExpr =
                (AbstractFunctionCallExpression) unnest.getExpressionRef().getValue();
        BTreeJobGenParams params = new BTreeJobGenParams();
        params.readFromFuncArgs(funcExpr.getArguments());
        List<LogicalVariable> probeVars = params.getLowKeyVarList();
        if (probeVars == null || probeVars.size() != sortColumns.length) {
            return false;
        }

        // check probe variables match order keys
        for (int i = 0; i < probeVars.size(); i++) {
            if (!probeVars.get(i).equals(sortColumns[i].getColumn())) {
                return false;
            }
        }

        // skip if input already delivers (G,K) order
        AbstractLogicalOperator sortInput =
                (AbstractLogicalOperator) orderOp.getInputs().get(0).getValue();
        if (alreadyOrdered(sortInput, sortColumns)) {
            return false;
        }

        IncrementalSortPOperator incremental = new IncrementalSortPOperator(sortColumns, probeVars);
        incremental.createLocalMemoryRequirements(orderOp, ctx.getPhysicalOptimizationConfig());
        orderOp.setPhysicalOperator(incremental);
        return true;
    }

    private boolean alreadyOrdered(AbstractLogicalOperator op, OrderColumn[] orderColumns) {
        StructuralPropertiesVector props = (StructuralPropertiesVector) op.getDeliveredPhysicalProperties();
        if (props == null) {
            return false;
        }
        List<ILocalStructuralProperty> locals = props.getLocalProperties();
        if (locals == null) {
            return false;
        }
        for (ILocalStructuralProperty p : locals) {
            if (p.getPropertyType() == ILocalStructuralProperty.PropertyType.LOCAL_ORDER_PROPERTY) {
                List<OrderColumn> delivered = ((LocalOrderProperty) p).getOrderColumns();
                if (delivered.size() < orderColumns.length) {
                    continue;
                }
                boolean match = true;
                for (int i = 0; i < orderColumns.length; i++) {
                    if (!delivered.get(i).getColumn().equals(orderColumns[i].getColumn())
                            || delivered.get(i).getOrder() != orderColumns[i].getOrder()) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    return true;
                }
            }
        }
        return false;
    }
}

