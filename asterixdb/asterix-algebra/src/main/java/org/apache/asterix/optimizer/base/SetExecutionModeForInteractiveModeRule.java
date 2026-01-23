package org.apache.asterix.optimizer.base;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;

// change result operator to a single partition in interactive mode
public class SetExecutionModeForInteractiveModeRule
        implements org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule {
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {

        return false;
    }

    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        if (!(opRef.getValue() instanceof DistributeResultOperator)) {
            return false;
        }
        if (!context.getPhysicalOptimizationConfig().getInteractiveMode())
            return false;
        ILogicalOperator op = opRef.getValue();
        while (op.getOperatorTag() != LogicalOperatorTag.GROUP) {
            for (int i = 0; i < op.getInputs().size(); i++) {
                AbstractLogicalOperator abstractOp = (AbstractLogicalOperator) op;
                abstractOp.setExecutionMode(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
                op = abstractOp.getInputs().get(i).getValue();

            }
        }
        return true;

    }
}
