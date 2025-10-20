package org.apache.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.*;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingFrameTupleReference;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy;

public class IncrementalSorterOperatorDescriptorTemp extends AbstractOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    protected static final int SORT_ACTIVITY_ID = 0;
    protected static final int MERGE_ACTIVITY_ID = 1;
    private static final int FRAME_SIZE = 32 * 1024;
    private EnumFreeSlotPolicy policy = EnumFreeSlotPolicy.LAST_FIT;

    protected final int[] sortFields;
    protected final int[] groupFields;
    private final RecordDescriptor inRecordDesc;

    protected final INormalizedKeyComputerFactory[] keyNormalizerFactories;
    protected final IBinaryComparatorFactory[] comparatorFactories;
    protected final int framesLimit;
    private Algorithm alg;
    private final int outputLimit;
    private boolean first;

    public IncrementalSorterOperatorDescriptorTemp(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            int[] groupFields, INormalizedKeyComputerFactory keyNormalizerFactory,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor, Algorithm alg,
            int outputLimit) {
        super(spec, 1, 1);

        this.sortFields = sortFields;
        this.keyNormalizerFactories = new INormalizedKeyComputerFactory[] { keyNormalizerFactory };
        this.groupFields = groupFields;
        this.comparatorFactories = comparatorFactories;
        this.framesLimit = framesLimit;
        this.inRecordDesc = recordDescriptor;
        this.alg = alg;
        this.outputLimit = outputLimit;

        INormalizedKeyComputerFactory[] keyNormalizerFactories =
                new INormalizedKeyComputerFactory[comparatorFactories.length];
        keyNormalizerFactories[comparatorFactories.length - 1] = keyNormalizerFactory;
        //sortOp = new ExternalSortOperatorDescriptor(spec,framesLimit,sortFields,keyNormalizerFactories,comparatorFactories,recordDescriptor, alg);

        // Match frame size

    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        IncrementalSorterNode isna = new IncrementalSorterNode(new ActivityId(getOperatorId(), 0));
        builder.addActivity(this, isna);
        for (int i = 0; i < inputArity; ++i) {
            builder.addSourceEdge(i, isna, i);
        }

        builder.addTargetEdge(0, isna, 0);
    }

    public class IncrementalSorterNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public IncrementalSorterNode(ActivityId id) {
            super(id);
        }

        @Override
        public ActivityId getActivityId() {
            return id;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {
            return new IncrementalSorter(ctx, recordDescProvider, partition, nPartitions, this.getActivityId());
        }
    }

    protected IRunGenerator getRunGenerator(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider)
            throws HyracksDataException {
        IRunGenerator runGen = new ExternalSortRunGenerator(ctx, sortFields, keyNormalizerFactories,
                comparatorFactories, outRecDescs[0], alg, policy, framesLimit, outputLimit);
        return runGen;
    }

    protected AbstractExternalSortRunMerger getSortRunMerger(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, List<GeneratedRunFileReader> runs,
            IBinaryComparator[] comparators, INormalizedKeyComputer nmkComputer, int necessaryFrames) {
        return new ExternalSortRunMerger(ctx, runs, sortFields, comparators, nmkComputer, outRecDescs[0],
                necessaryFrames, outputLimit);
    }

    private class IncrementalSorter extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
        private static final long serialVersionUID = 1L;
        private final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        private IRunGenerator runGen;
        IHyracksTaskContext ctx;
        IRecordDescriptorProvider recordDescProvider;
        int partition;
        int nPartitions;
        private final ActivityId activityId;
        private final PermutingFrameTupleReference groupFieldsRef;
        ByteBuffer tempBuffer = ByteBuffer.allocate(FRAME_SIZE);
        private final FrameTupleAccessor inFrameAccessor;
        //private final IBinaryComparator[] comparators;

        public IncrementalSorter(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider, int partition,
                int nPartitions, ActivityId activityId) {
            this.ctx = ctx;
            this.recordDescProvider = recordDescProvider;
            this.partition = partition;
            this.nPartitions = nPartitions;
            this.activityId = activityId;
            this.groupFieldsRef = new PermutingFrameTupleReference(groupFields);
            for (int j = 0; j < comparatorFactories.length; j++) {
                comparators[j] = comparatorFactories[j].createBinaryComparator();
            }
            this.inFrameAccessor = new FrameTupleAccessor(inRecordDesc);

        }

        @Override
        public void open() throws HyracksDataException {
            runGen = getRunGenerator(ctx, recordDescProvider);
            runGen.open();
            first = true;

        }

        @Override
        public void nextFrame(ByteBuffer frame) throws HyracksDataException {
            // Initialize a temporary output buffer if not already done
            if (tempBuffer == null) {
                tempBuffer = ByteBuffer.allocate(frame.capacity());
            }

            // Clear temp buffer only at start of new frame (safe now)
            tempBuffer.clear();

            // Reset frame accessor to read tuples from the incoming frame
            inFrameAccessor.reset(frame);
            int tupleCount = inFrameAccessor.getTupleCount();

            for (int i = 0; i < tupleCount; i++) {
                // Check if driving key boundary is reached before buffering
                if (!first && isDrivingFactorReached(groupFieldsRef, inFrameAccessor, i, groupFields, comparators)) {
                    // Flush previous group
                    flushBuffer();

                    // Close current run and merge
                    AbstractSorterOperatorDescriptor.SortTaskState state =
                            new AbstractSorterOperatorDescriptor.SortTaskState(ctx.getJobletContext().getJobId(),
                                    new TaskId(activityId, partition));

                    List<GeneratedRunFileReader> runs = closeCurrentRunGeneration(state);
                    mergeGeneratedRuns(runs, state, ctx, recordDescProvider, writer);

                    // Start a new run generator
                    runGen = getRunGenerator(ctx, recordDescProvider);
                    runGen.open();

                    // Optional: clear buffer explicitly again for clarity
                    tempBuffer.clear();
                }

                first = false;

                // Get the tuple bytes
                int tupleStart = inFrameAccessor.getTupleStartOffset(i);
                int tupleLen = inFrameAccessor.getTupleLength(i);

                // Ensure enough space in buffer before putting tuple
                if (tempBuffer.remaining() < tupleLen) {
                    flushBuffer(); // Flush to make room
                }

                // Buffer the current tuple
                tempBuffer.put(frame.array(), tupleStart, tupleLen);
            }

            // Flush any remaining buffered tuples at end of frame
            if (tempBuffer.position() > 0) {
                flushBuffer();
            }
        }

        @Override
        public void fail() throws HyracksDataException {
            runGen.fail();

        }

        @Override
        public void close() throws HyracksDataException {
            AbstractSorterOperatorDescriptor.SortTaskState state = new AbstractSorterOperatorDescriptor.SortTaskState(
                    ctx.getJobletContext().getJobId(), new TaskId(activityId, partition));
            List<GeneratedRunFileReader> runs = closeCurrentRunGeneration(state);
            mergeGeneratedRuns(runs, state, ctx, recordDescProvider, writer);

        }

        private List<GeneratedRunFileReader> closeCurrentRunGeneration(
                AbstractSorterOperatorDescriptor.SortTaskState state) throws HyracksDataException {

            runGen.close();
            state.generatedRunFileReaders = runGen.getRuns();
            List<GeneratedRunFileReader> runs = state.generatedRunFileReaders;
            state.sorter = runGen.getSorter();
            ctx.setStateObject(state);
            return runs;

        }

        private void flushBuffer() throws HyracksDataException {
            if (tempBuffer.position() > 0) {
                tempBuffer.flip(); // Prepare for reading/output
                runGen.nextFrame(tempBuffer); // Emit buffered tuples
                tempBuffer.clear(); // Reset buffer for new data
            }
        }

    }

    //    private void switchGroupsIfRequired(ITupleReference prevTupleGroupFields, IFrameTupleAccessor currTupleAccessor,
    //                                        int currTupleIndex){
    //        if (isDrivingFactorReached(groupFields,comparators)){
    //
    //            AbstractSorterOperatorDescriptor.SortTaskState state = new AbstractSorterOperatorDescriptor.SortTaskState(ctx.getJobletContext().getJobId(),
    //                    new TaskId(activityId, partition));
    //            List<GeneratedRunFileReader> runs = closeCurrentRunGeneration(state);
    //            mergeGeneratedRuns(runs, state, ctx, recordDescProvider, writer);
    //            runGen = getRunGenerator(ctx, recordDescProvider);
    //            runGen.open();
    //            runGen.nextFrame(buffer);
    //
    //
    //
    //        }
    //        else{
    //            runGen.nextFrame(buffer);
    //        }
    //
    //
    //    }

    private boolean isDrivingFactorReached(ITupleReference prevTupleGroupFields, IFrameTupleAccessor curTupleAccessor,
            int curTupleIdx, int[] curTupleGroupFields, IBinaryComparator[] comparators) throws HyracksDataException {

        for (int i = 0; i < comparators.length; ++i) {
            byte[] prevTupleFieldData = prevTupleGroupFields.getFieldData(i);
            int prevTupleFieldStart = prevTupleGroupFields.getFieldStart(i);
            int prevTupleFieldLength = prevTupleGroupFields.getFieldLength(i);

            byte[] curTupleFieldData = curTupleAccessor.getBuffer().array();
            int curTupleFieldIdx = curTupleGroupFields[i];
            int curTupleFieldStart = curTupleAccessor.getAbsoluteFieldStartOffset(curTupleIdx, curTupleFieldIdx);
            int curTupleFieldLength = curTupleAccessor.getFieldLength(curTupleIdx, curTupleFieldIdx);

            if (comparators[i].compare(prevTupleFieldData, prevTupleFieldStart, prevTupleFieldLength, curTupleFieldData,
                    curTupleFieldStart, curTupleFieldLength) != 0) {
                return true;
            }
        }
        return false;
    }

    private void mergeGeneratedRuns(List<GeneratedRunFileReader> runs,
            AbstractSorterOperatorDescriptor.SortTaskState state, IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, IFrameWriter writer) throws HyracksDataException {
        ISorter sorter = state.sorter;
        IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        INormalizedKeyComputer nmkComputer =
                keyNormalizerFactories == null ? null : keyNormalizerFactories[0].createNormalizedKeyComputer();
        AbstractExternalSortRunMerger merger = null;
        merger = getSortRunMerger(ctx, recordDescProvider, runs, comparators, nmkComputer, framesLimit);
        IFrameWriter wrappingWriter = null;
        try {
            if (runs.isEmpty()) {
                wrappingWriter = merger.prepareSkipMergingFinalResultWriter(writer);
                wrappingWriter.open();
                if (sorter.hasRemaining()) {
                    sorter.flush(wrappingWriter);
                }
            } else {
                // eagerly close the sorter here to release memory rather than in finally
                sorter.close();
                sorter = null;
                wrappingWriter = merger.prepareFinalMergeResultWriter(writer);
                wrappingWriter.open();
                merger.process(wrappingWriter);
            }
        } catch (Throwable e) {
            if (wrappingWriter != null) {
                wrappingWriter.fail();
            }
            throw HyracksDataException.create(e);
        } finally {
            if (sorter != null) {
                sorter.close();
            }
            if (wrappingWriter != null) {
                wrappingWriter.close();
            }
        }
    }

}
