package org.apache.asterix.optimizer.rules;

import org.apache.asterix.algebra.operators.physical.PlaqueAggregatePOperator;
import org.apache.asterix.algebra.operators.physical.PlaqueFilterPOperator;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PlaqueRewriteRule implements IAlgebraicRewriteRule {

    private static final String PLAQUE_FILTER = "plaque-filter";
    private static final String PLAQUE_AGGREGATE = "plaque-aggregate";
    private static final String PLAQUE_HANDLE = "plaque-handle";

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        if (!context.getPhysicalOptimizationConfig().getPlaqueMode()) {
            return false;
        }

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }

        AggregateOperator aggOp = (AggregateOperator) op;

        if (Boolean.TRUE.equals(aggOp.getAnnotations().get(PLAQUE_AGGREGATE))) {
            return false;
        }

        if (aggOp.getExpressions().size() != 1) {
            return false;
        }

        ILogicalExpression expr = aggOp.getExpressions().get(0).getValue();
        if (!(expr instanceof AggregateFunctionCallExpression)) {
            return false;
        }

        AggregateFunctionCallExpression aggExpr = (AggregateFunctionCallExpression) expr;
        FunctionIdentifier fid = aggExpr.getFunctionIdentifier();

        boolean isMax;
        if (fid.equals(BuiltinFunctions.LOCAL_SQL_MAX)) {
            isMax = true;
        } else if (fid.equals(BuiltinFunctions.LOCAL_SQL_MIN)) {
            isMax = false;
        } else {
            return false;
        }

        if (aggExpr.getArguments().size() != 1) {
            return false;
        }

        ILogicalExpression arg = aggExpr.getArguments().get(0).getValue();
        if (!(arg instanceof VariableReferenceExpression)) {
            return false;
        }

        LogicalVariable filteredVar = ((VariableReferenceExpression) arg).getVariableReference();

        String handle = "plaque-" + context.newVar();

        ILogicalOperator childOp = aggOp.getInputs().get(0).getValue();

        SelectOperator plaqueSelect =
                new SelectOperator(new MutableObject<>(ConstantExpression.TRUE));

        plaqueSelect.setSourceLocation(aggOp.getSourceLocation());
        plaqueSelect.setExecutionMode(((AbstractLogicalOperator) childOp).getExecutionMode());

        plaqueSelect.getAnnotations().put(PLAQUE_FILTER, true);
        plaqueSelect.getAnnotations().put(PLAQUE_HANDLE, handle);
        plaqueSelect.setPhysicalOperator(new PlaqueFilterPOperator(filteredVar, isMax, handle));

        plaqueSelect.getInputs().add(new MutableObject<>(childOp));
        aggOp.getInputs().set(0, new MutableObject<>(plaqueSelect));

        aggOp.getAnnotations().put(PLAQUE_AGGREGATE, true);
        aggOp.getAnnotations().put(PLAQUE_HANDLE, handle);
        aggOp.setPhysicalOperator(new PlaqueAggregatePOperator(handle, isMax));

        return true;
    }
}
