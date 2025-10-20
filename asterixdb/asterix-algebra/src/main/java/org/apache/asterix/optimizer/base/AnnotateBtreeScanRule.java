package org.apache.asterix.optimizer.base;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class AnnotateBtreeScanRule implements IAlgebraicRewriteRule {
    private static boolean hasAnnotated = false;
    private static final String RULE_ALREADY_APPLIED_KEY = "hasAnnotatedLeftMostBtree";

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {

        if (hasAnnotated || !context.getPhysicalOptimizationConfig().getInteractiveMode()) {
            return false;
        }

        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.UNNEST_MAP)
            return false;

        // Only annotate the first (i.e., deepest/leftmost) btree we find
        if (!context.checkIfInDontApplySet(this, op)) {
            op.getAnnotations().put("Left_Most_Btree", true);
            context.addToDontApplySet(this, op);
            hasAnnotated = true;// prevent annotating again
            return true;
        }

        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

}
