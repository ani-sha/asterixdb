package org.apache.hyracks.dataflow.std.group.sorted;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

/**
 * Operator that performs grouping assuming sorted input where each frame contains only one key.
 */
public class SortedFrameGroupByOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final int groupField; // Single grouping key field
    private final IAggregatorDescriptorFactory aggregatorFactory;
    private final RecordDescriptor aggRecordDesc;
    private final RecordDescriptor outputRecordDesc;
    private final int framesLimit;

    public SortedFrameGroupByOperatorDescriptor(IOperatorDescriptorRegistry spec, int groupField,
            IAggregatorDescriptorFactory aggregatorFactory, RecordDescriptor aggDesc, RecordDescriptor outputDesc,
            int framesLimit) {
        super(spec, 1, 1);
        this.groupField = groupField;
        this.aggregatorFactory = aggregatorFactory;
        this.aggRecordDesc = aggDesc;
        this.outputRecordDesc = outputDesc;
        this.framesLimit = framesLimit;
    }

    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx, final int partition,
            final IRecordDescriptorProvider recordDescProvider, final int framesLimit) throws HyracksDataException {
        return new SortedFrameGroupByOperatorNodePushable(ctx, groupField, aggregatorFactory, aggRecordDesc,
                outputRecordDesc, framesLimit);
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {

    }
}
