package org.apache.hyracks.dataflow.std.group.sorted;

import static org.apache.hyracks.dataflow.common.comm.util.FrameUtils.appendToWriter;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.group.AggregateState;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptor;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class SortedFrameGroupByOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    private final IHyracksTaskContext ctx;
    private final int groupField;
    private final IAggregatorDescriptor aggregator;
    private final RecordDescriptor inputRecordDesc;
    private final RecordDescriptor outputRecordDesc;
    private final ArrayTupleBuilder tupleBuilder;
    private final long memoryLimit;
    private FrameTupleAccessor tupleAccessor;
    private Object currentGroupKey = null;
    private AggregateState aggregationState;
    private final FrameTupleAppender tupleAppender = new FrameTupleAppender();
    private final IFrame frame;
    // Track the current group key

    public SortedFrameGroupByOperatorNodePushable(IHyracksTaskContext ctx, int groupField,
            IAggregatorDescriptorFactory aggregatorFactory, RecordDescriptor inputRecordDesc,
            RecordDescriptor outputRecordDesc, int framesLimit) throws HyracksDataException {
        this.ctx = ctx;
        this.groupField = groupField;
        this.memoryLimit = framesLimit <= 0 ? -1 : ((long) (framesLimit - 2)) * ctx.getInitialFrameSize();
        int[] groupfields = new int[] { groupField };

        this.aggregator = aggregatorFactory.createAggregator(ctx, inputRecordDesc, outputRecordDesc, groupfields,
                groupfields, writer, memoryLimit);

        this.inputRecordDesc = inputRecordDesc;
        this.outputRecordDesc = outputRecordDesc;
        this.tupleAccessor = new FrameTupleAccessor(inputRecordDesc);
        tupleBuilder = new ArrayTupleBuilder(outputRecordDesc.getFields().length);
        this.frame = new VSizeFrame(ctx);
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
        tupleAccessor = new FrameTupleAccessor(inputRecordDesc);
        aggregationState = aggregator.createAggregateStates();
        currentGroupKey = null;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        tupleAccessor.reset(buffer);
        // Attach buffer to the accessor

        int tupleCount = tupleAccessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            // Extract the key from the current tuple
            int startOffset = tupleAccessor.getTupleStartOffset(i);
            int keyOffset = tupleAccessor.getFieldStartOffset(i, groupField);
            int keyLength = tupleAccessor.getFieldLength(i, groupField);

            Object newKey = extractKey(tupleAccessor, i, groupField);

            if (currentGroupKey == null || !currentGroupKey.equals(newKey)) {
                // Finalize previous group aggregation
                if (currentGroupKey != null) {
                    tupleBuilder.reset();
                    if (aggregator.outputFinalResult(tupleBuilder, tupleAccessor, i, aggregationState)) {
                        appendToWriter();
                    }
                }

                aggregator.reset();
                aggregationState = aggregator.createAggregateStates();

                // Initialize aggregation with the first tuple of the new group
                tupleBuilder.reset();
                aggregator.init(tupleBuilder, tupleAccessor, i, aggregationState);

                // Update the current group key
                currentGroupKey = newKey;
            }

            // Aggregate current tuple
            aggregator.aggregate(tupleAccessor, i, null, -1, aggregationState);
        }
    }

    @Override
    public void fail() throws HyracksDataException {

    }

    @Override
    public void close() throws HyracksDataException {
        if (currentGroupKey != null) {

            tupleBuilder.reset();
            if (aggregator.outputFinalResult(tupleBuilder, tupleAccessor, 0, aggregationState)) {
                appendToWriter();
            }
        }

        writer.close();
    }

    private Object extractKey(FrameTupleAccessor accessor, int tupleIndex, int fieldIndex) {
        // Extracts the grouping key as a byte array (can be modified based on actual key type)
        int keyStart = accessor.getFieldStartOffset(tupleIndex, fieldIndex);
        int keyLength = accessor.getFieldLength(tupleIndex, fieldIndex);
        byte[] keyBytes = new byte[keyLength];
        System.arraycopy(accessor.getBuffer().array(), keyStart, keyBytes, 0, keyLength);
        return keyBytes; // Can be further deserialized based on data type
    }

    @Override
    public void initialize() throws HyracksDataException {

    }

    private void appendToWriter() throws HyracksDataException {

        tupleAppender.reset(frame, true);
        tupleAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0, tupleBuilder.getSize());
        FrameUtils.flushFrame(frame.getBuffer(), writer);

    }

    @Override
    public void deinitialize() throws HyracksDataException {

    }

    @Override
    public String getDisplayName() {
        return "";
    }
}
