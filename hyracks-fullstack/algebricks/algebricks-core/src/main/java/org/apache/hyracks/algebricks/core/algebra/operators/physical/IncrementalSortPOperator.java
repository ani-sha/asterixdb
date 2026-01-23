package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import static org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag.INCREMENTAL_SORT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.*;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.*;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.sort.IncrementalSortOperatorDescriptor;

public class IncrementalSortPOperator extends AbstractPhysicalOperator {
    public static final int MIN_FRAME_LIMIT_FOR_SORT = 3;

    OrderColumn[] sortColumns;
    protected List<LogicalVariable> groupByColumns;

    ILocalStructuralProperty orderProp;

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return INCREMENTAL_SORT;
    }

    public IncrementalSortPOperator(OrderColumn[] sortColumns, List<LogicalVariable> columnList) {
        this.sortColumns = sortColumns;
        this.groupByColumns = columnList;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext ctx) throws AlgebricksException {
        // the input is expected to already be ordered by the grouping key. No
        // additional requirements are enforced here and the rewrite rule is
        // responsible for ensuring the pre-condition.
        return emptyUnaryRequirements();
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator childOp = (AbstractLogicalOperator) op.getInputs().get(0).getValue();

        List<OrderColumn> orderCols = new ArrayList<>();
        for (LogicalVariable gVar : groupByColumns) {
            orderCols.add(new OrderColumn(gVar, OrderOperator.IOrder.OrderKind.ASC));
        }
        orderCols.addAll(Arrays.asList(sortColumns));
        orderProp = new LocalOrderProperty(orderCols);

        StructuralPropertiesVector childProp = (StructuralPropertiesVector) childOp.getDeliveredPhysicalProperties();
        deliveredProperties = new StructuralPropertiesVector(childProp.getPartitioningProperty(),
                Collections.singletonList(orderProp));
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        IOperatorDescriptorRegistry spec = builder.getJobSpec();
        RecordDescriptor recDescriptor =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), opSchema, context);
        int n = sortColumns.length;
        int[] sortFields = new int[n];
        int m = groupByColumns.size();
        int[] groupByFields = new int[m];

        IBinaryComparatorFactory[] sortComps = new IBinaryComparatorFactory[n];
        IBinaryComparatorFactory[] groupComps = new IBinaryComparatorFactory[m];

        IVariableTypeEnvironment env = context.getTypeEnvironment(op);
        int i = 0;
        for (OrderColumn oc : sortColumns) {
            LogicalVariable var = oc.getColumn();
            sortFields[i] = opSchema.findVariable(var);
            Object type = env.getVarType(var);
            IBinaryComparatorFactoryProvider bcfp = context.getBinaryComparatorFactoryProvider();
            sortComps[i] = bcfp.getBinaryComparatorFactory(type, oc.getOrder() == OrderOperator.IOrder.OrderKind.ASC);
            i++;

        }
        i = 0;
        for (LogicalVariable var : groupByColumns) {
            groupByFields[i] = opSchema.findVariable(var);
            Object type = env.getVarType(var);
            IBinaryComparatorFactoryProvider bcfp = context.getBinaryComparatorFactoryProvider();
            groupComps[i] = bcfp.getBinaryComparatorFactory(type, true);
            i++;
        }
        // Always ASC for group keys

        int framesLimit = localMemoryRequirements.getMemoryBudgetInFrames();
        IBinaryComparatorFactory groupCmpFactory = groupComps.length > 0 ? groupComps[0] : null;

        IncrementalSortOperatorDescriptor incrementalSort = new IncrementalSortOperatorDescriptor(spec,
                groupByFields[0], sortFields, groupCmpFactory, sortComps, framesLimit, null, recDescriptor);
        incrementalSort.setSourceLocation(op.getSourceLocation());
        contributeOpDesc(builder, (AbstractLogicalOperator) op, incrementalSort);
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

    @Override
    public void createLocalMemoryRequirements(ILogicalOperator op) {
        localMemoryRequirements = LocalMemoryRequirements.variableMemoryBudget(MIN_FRAME_LIMIT_FOR_SORT);
    }

    @Override
    public void createLocalMemoryRequirements(ILogicalOperator op, PhysicalOptimizationConfig physicalOpConfig) {
        localMemoryRequirements = LocalMemoryRequirements.variableMemoryBudget(physicalOpConfig.getMinSortFrames());
    }
}