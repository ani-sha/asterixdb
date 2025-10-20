package org.apache.asterix.optimizer.base;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;

public class AnnotateDynamicFilterRule
        implements org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule {

    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {

        return false;
    }

    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (!context.getPhysicalOptimizationConfig().getBlockingMode())
            return false;
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() == LogicalOperatorTag.DISTRIBUTE_RESULT) {
            while (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
                op = op.getInputs().get(0).getValue();
            }
            op.getAnnotations().put("Is_Dynamic_Filter", true);
            return true;
        }

        return false;
    }
}
