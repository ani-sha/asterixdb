package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.*;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.*;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.EagerMergeOperatorDescriptor;

public class EagerMergePOperator extends AbstractPhysicalOperator {
    protected List<LogicalVariable> groupByColumns;

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.EAGER_MERGE;
    }

    public EagerMergePOperator(List<LogicalVariable> groupByColumns) {
        this.groupByColumns = groupByColumns;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) throws AlgebricksException {
        return emptyUnaryRequirements();
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator inputOp = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        IPhysicalPropertiesVector inputProps = inputOp.getPhysicalOperator().getDeliveredProperties();
        deliveredProperties = inputProps.clone();
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        IOperatorDescriptorRegistry spec = builder.getJobSpec();
        RecordDescriptor recDescriptor =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), propagatedSchema, context);
        int m = groupByColumns.size();
        int[] groupByFields = new int[m];
        IBinaryComparatorFactory[] groupComps = new IBinaryComparatorFactory[m];
        IVariableTypeEnvironment env = context.getTypeEnvironment(op);
        int i = 0;
        for (LogicalVariable var : groupByColumns) {
            groupByFields[i] = propagatedSchema.findVariable(var);
            Object type = env.getVarType(var);
            IBinaryComparatorFactoryProvider bcfp = context.getBinaryComparatorFactoryProvider();
            groupComps[i] = bcfp.getBinaryComparatorFactory(type, true);
            i++;
        }
        EagerMergeOperatorDescriptor eagerMergeOperatorDescriptor =
                new EagerMergeOperatorDescriptor(spec, recDescriptor, groupComps, groupByFields);
        contributeOpDesc(builder, (AbstractLogicalOperator) op, eagerMergeOperatorDescriptor);
        ILogicalOperator src = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, op, 0);

    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return false;
    }
}
