package org.apache.asterix.optimizer.base;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.*;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.PreclusteredGroupByPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.SortGroupByPOperator;

public class ReplaceSortGroupBywithPreClusteredGroupByRule
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
        return rewriteSortGroupByRecursively(opRef);
    }

    private boolean rewriteSortGroupByRecursively(Mutable<ILogicalOperator> opRef) throws AlgebricksException {
        boolean rewritten = false;
        ILogicalOperator op = opRef.getValue();

        if (op.getOperatorTag() == LogicalOperatorTag.GROUP) {
            AbstractLogicalOperator abstractOp = (AbstractLogicalOperator) op;
            if (abstractOp.getPhysicalOperator().getOperatorTag() == PhysicalOperatorTag.SORT_GROUP_BY) {
                SortGroupByPOperator sortGroupByPOperator = (SortGroupByPOperator) abstractOp.getPhysicalOperator();
                GroupByOperator groupByOperator = (GroupByOperator) abstractOp;
                boolean groupAll = groupByOperator.isGroupAll();
                List<LogicalVariable> columnList = sortGroupByPOperator.getGroupByColumns();

                PreclusteredGroupByPOperator preclustered = new PreclusteredGroupByPOperator(columnList, groupAll);
                abstractOp.setPhysicalOperator(preclustered);
                rewritten = true;
            }
        }
        if (op.getOperatorTag() == LogicalOperatorTag.GROUP && !op.getInputs().isEmpty()) {
            ILogicalOperator child = op.getInputs().get(0).getValue();
            if (child.getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
                List<Mutable<ILogicalOperator>> grandChildren = child.getInputs();
                if (!grandChildren.isEmpty()
                        && grandChildren.get(0).getValue().getOperatorTag() == LogicalOperatorTag.GROUP) {
                    // Global group-by detected
                    op.getAnnotations().put("isGlobal", true);
                }
            }
        }

        // Recurse into children
        for (Mutable<ILogicalOperator> input : op.getInputs()) {
            rewritten |= rewriteSortGroupByRecursively(input);
        }

        return rewritten;
    }

    private boolean findSortAndChangeGroupBy(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        //        if(op.getOperatorTag() == LogicalOperatorTag.GROUP && op.getAnnotations().containsKey("right_Side_of_Union")){
        //            if(op.getPhysicalOperator().getOperatorTag() == PhysicalOperatorTag.SORT_GROUP_BY){
        //                SortGroupByPOperator sortGroupByPOperator = (SortGroupByPOperator) op.getPhysicalOperator();
        //                List<LogicalVariable> columnList = sortGroupByPOperator.getGroupByColumns();
        //                GroupByOperator groupByOperator= (GroupByOperator)op;
        //                boolean groupAll = groupByOperator.isGroupAll();
        //                PreclusteredGroupByPOperator preclusteredGroupByPOperator = new PreclusteredGroupByPOperator(columnList, groupAll);
        //                op.setPhysicalOperator(preclusteredGroupByPOperator);
        //                return true;
        //
        //
        //            }
        //        }
        //If you want interactivity in single query
        if (op.getOperatorTag() == LogicalOperatorTag.GROUP) {
            if (op.getPhysicalOperator().getOperatorTag() == PhysicalOperatorTag.SORT_GROUP_BY) {
                SortGroupByPOperator sortGroupByPOperator = (SortGroupByPOperator) op.getPhysicalOperator();
                List<LogicalVariable> columnList = sortGroupByPOperator.getGroupByColumns();
                GroupByOperator groupByOperator = (GroupByOperator) op;
                boolean groupAll = groupByOperator.isGroupAll();
                PreclusteredGroupByPOperator preclusteredGroupByPOperator =
                        new PreclusteredGroupByPOperator(columnList, groupAll);
                op.setPhysicalOperator(preclusteredGroupByPOperator);
                return true;

            }
        }

        return false;
    }
}
