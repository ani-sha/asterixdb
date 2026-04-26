package org.apache.asterix.algebra.operators.physical;

import org.apache.asterix.runtime.operators.plaque.PlaqueFilterRuntimeFactory;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;

public class PlaqueFilterPOperator extends AbstractPhysicalOperator {

    private final LogicalVariable filteredVar;
    private final boolean isMax;
    private final String handle;

    public PlaqueFilterPOperator(LogicalVariable filteredVar, boolean isMax, String handle) {
        this.filteredVar = filteredVar;
        this.isMax = isMax;
        this.handle = handle;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.PLAQUE_FILTER;
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        ILogicalOperator child = op.getInputs().get(0).getValue();
        deliveredProperties = ((AbstractLogicalOperator) child).getDeliveredPhysicalProperties();
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
                                                                 IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        return emptyUnaryRequirements();
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context,
                                          ILogicalOperator op, IOperatorSchema opSchema, IOperatorSchema[] inputSchemas,
                                          IOperatorSchema outerPlanSchema) throws AlgebricksException {

        int columnIdx = inputSchemas[0].findVariable(filteredVar);
        if (columnIdx < 0) {
            throw new AlgebricksException("Filtered variable not found in input schema: " + filteredVar);
        }

        PlaqueFilterRuntimeFactory runtime =
                new PlaqueFilterRuntimeFactory(columnIdx, isMax, handle);

        RecordDescriptor recDesc =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), opSchema, context);

        builder.contributeMicroOperator(op, runtime, recDesc);

        ILogicalOperator src = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, op, 0);
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return false;
    }

    @Override
    public String toString() {
        return "PLAQUE_FILTER(" + filteredVar + ", " + (isMax ? "MAX" : "MIN") + ", " + handle + ")";
    }
}