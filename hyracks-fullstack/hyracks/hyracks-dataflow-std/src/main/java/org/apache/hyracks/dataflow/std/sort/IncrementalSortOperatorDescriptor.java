package org.apache.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy;

/**
 * Runtime descriptor for the incremental (group-local) sorter. Tuples are assumed to arrive
 * clustered by a group key. For each group the tuples are sorted by the provided order fields
 * and immediately emitted before moving to the next group.
 */
public class IncrementalSortOperatorDescriptor extends AbstractOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final int groupKeyPos;
    private final int[] orderFields;
    private final IBinaryComparatorFactory groupComparatorFactory;
    private final IBinaryComparatorFactory[] orderComparatorFactories;
    private final int frameLimit;
    private final Integer topK;
    private final RecordDescriptor inRecordDesc;

    public IncrementalSortOperatorDescriptor(IOperatorDescriptorRegistry spec, int groupKeyPos, int[] orderFields,
            IBinaryComparatorFactory groupComparatorFactory, IBinaryComparatorFactory[] orderComparatorFactories,
            int frameLimit, Integer topK, RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        this.groupKeyPos = groupKeyPos;
        this.orderFields = orderFields;
        this.groupComparatorFactory = groupComparatorFactory;
        this.orderComparatorFactories = orderComparatorFactories;
        this.frameLimit = frameLimit;
        this.topK = topK;
        this.inRecordDesc = recordDescriptor;
        this.outRecDescs[0] = recordDescriptor;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        IncrementalSortActivityNode activity = new IncrementalSortActivityNode(new ActivityId(getOperatorId(), 0));
        builder.addActivity(this, activity);
        builder.addSourceEdge(0, activity, 0);
        builder.addTargetEdge(0, activity, 0);
    }

    private class IncrementalSortActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        IncrementalSortActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider recordDescProvider, int partition,
                int nPartitions) throws HyracksDataException {
            return new IncrementalSortOperatorNodePushable(ctx, getActivityId(), partition);
        }
    }

    private class IncrementalSortOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

        private final IHyracksTaskContext ctx;
        private final ActivityId activityId;
        private final int partition;

        private final FrameTupleAccessor accessor;
        private final IBinaryComparator groupComparator;
        private final IBinaryComparator[] sortComparators;

        private IRunGenerator runGen;
        private FrameTupleAppender appender;
        private VSizeFrame outFrame;

        private byte[] currentGroupKey;
        private boolean haveGroup;

        IncrementalSortOperatorNodePushable(IHyracksTaskContext ctx, ActivityId activityId, int partition)
                throws HyracksDataException {
            this.ctx = ctx;
            this.activityId = activityId;
            this.partition = partition;
            this.accessor = new FrameTupleAccessor(inRecordDesc);
            this.groupComparator = groupComparatorFactory.createBinaryComparator();
            this.sortComparators = new IBinaryComparator[orderComparatorFactories.length];
            for (int i = 0; i < orderComparatorFactories.length; i++) {
                sortComparators[i] = orderComparatorFactories[i].createBinaryComparator();
            }
        }

        @Override
        public void open() throws HyracksDataException {
            writer.open();
            runGen = createRunGenerator(ctx);
            runGen.open();
            outFrame = new VSizeFrame(ctx);
            appender = new FrameTupleAppender(outFrame, true);
            haveGroup = false;
            currentGroupKey = null;
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            accessor.reset(buffer);
            int tCount = accessor.getTupleCount();
            for (int t = 0; t < tCount; t++) {
                if (!haveGroup) {
                    latchCurrentGroup(accessor, t);
                    haveGroup = true;
                } else if (!sameGroup(accessor, t)) {
                    flushCurrentGroup();
                    latchCurrentGroup(accessor, t);
                }
                if (!appender.append(accessor, t)) {
                    flushAppender();
                    if (!appender.append(accessor, t)) {
                        throw new HyracksDataException("Tuple too large to fit in frame");
                    }
                }
            }
        }

        @Override
        public void close() throws HyracksDataException {
            Throwable failure = null;
            try {
                if (haveGroup) {
                    flushCurrentGroup();
                }
                writer.close();
            } catch (Throwable th) {
                failure = th;
            } finally {
                if (runGen != null) {
                    runGen.close();
                }
            }
            if (failure != null) {
                throw HyracksDataException.create(failure);
            }
        }

        @Override
        public void fail() throws HyracksDataException {
            if (runGen != null) {
                runGen.fail();
            }
            writer.fail();
        }

        private void latchCurrentGroup(FrameTupleAccessor acc, int index) {
            int start = acc.getAbsoluteFieldStartOffset(index, groupKeyPos);
            int len = acc.getFieldLength(index, groupKeyPos);
            currentGroupKey = Arrays.copyOfRange(acc.getBuffer().array(), start, start + len);
        }

        private boolean sameGroup(FrameTupleAccessor acc, int index) throws HyracksDataException {
            int start = acc.getAbsoluteFieldStartOffset(index, groupKeyPos);
            int len = acc.getFieldLength(index, groupKeyPos);
            return groupComparator.compare(currentGroupKey, 0, currentGroupKey.length, acc.getBuffer().array(), start,
                    len) == 0;
        }

        private void flushAppender() throws HyracksDataException {
            if (appender.getTupleCount() > 0) {
                appender.write(runGen, true);
                appender.reset(outFrame, true);
            }
        }

        private void flushCurrentGroup() throws HyracksDataException {
            flushAppender();
            runGen.close();
            AbstractSorterOperatorDescriptor.SortTaskState state = new AbstractSorterOperatorDescriptor.SortTaskState(
                    ctx.getJobletContext().getJobId(), new TaskId(activityId, partition));
            state.generatedRunFileReaders = runGen.getRuns();
            state.sorter = runGen.getSorter();

            if (state.generatedRunFileReaders.isEmpty()) {
                state.sorter.sort();
                state.sorter.flush(writer);
            } else {
                state.sorter.close();
                ExternalSortRunMerger merger =
                        new ExternalSortRunMerger(ctx, state.generatedRunFileReaders, orderFields, sortComparators,
                                null, inRecordDesc, frameLimit, topK == null ? Integer.MAX_VALUE : topK);
                merger.process(writer);
            }

            // clean up for next group
            for (GeneratedRunFileReader r : state.generatedRunFileReaders) {
                r.close();
            }
            runGen = createRunGenerator(ctx);
            runGen.open();
            appender.reset(outFrame, true);
            haveGroup = false;
            currentGroupKey = null;
        }

        private IRunGenerator createRunGenerator(IHyracksTaskContext ctx) throws HyracksDataException {
            return new ExternalSortRunGenerator(ctx, orderFields, null, orderComparatorFactories, inRecordDesc,
                    Algorithm.MERGE_SORT, EnumFreeSlotPolicy.LAST_FIT, frameLimit,
                    topK == null ? Integer.MAX_VALUE : topK);
        }
    }
}
