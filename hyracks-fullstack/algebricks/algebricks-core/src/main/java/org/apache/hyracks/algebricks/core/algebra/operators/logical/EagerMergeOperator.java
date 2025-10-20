package org.apache.hyracks.algebricks.core.algebra.operators.logical;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class EagerMergeOperator extends AbstractLogicalOperator {

    private final List<LogicalVariable> groupByColumns;

    public EagerMergeOperator(List<LogicalVariable> groupByColumns) {
        this.groupByColumns = groupByColumns;
    }

    public List<LogicalVariable> getGroupByColumns() {
        return groupByColumns;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.EAGER_MERGE;
    }

    @Override
    public void recomputeSchema() throws AlgebricksException {
        List<LogicalVariable> inputSchema = inputs.get(0).getValue().getSchema();
        schema = new ArrayList<>(inputSchema);

        for (LogicalVariable var : groupByColumns) {
            if (!schema.contains(var)) {
                schema.add(var);
            }
        }
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform transform)
            throws AlgebricksException {
        return false;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitEagerMergeOperator(this, arg);
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.ALL;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        return createPropagatingAllInputsTypeEnvironment(ctx);
    }
}
