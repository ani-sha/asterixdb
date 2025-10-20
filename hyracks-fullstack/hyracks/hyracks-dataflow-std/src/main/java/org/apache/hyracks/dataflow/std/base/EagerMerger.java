package org.apache.hyracks.dataflow.std.base;

import static org.apache.hyracks.dataflow.common.comm.util.FrameUtils.flushFrame;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class EagerMerger extends AbstractUnaryOutputOperatorNodePushable {
    PriorityQueue<HeapEntry> minHeap;
    int numPartitions;
    PartitionCursor[] cursors;
    private final Object lock = new Object();
    private final IHyracksTaskContext ctx;
    private final RecordDescriptor recordDesc;
    private FrameTupleAppender appender;
    private IFrame outputFrame;
    private final boolean[] inputDone;
    private int numCompleted = 0;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IBinaryComparator[] comparators;
    private final int[] sortFields;

    public EagerMerger(IHyracksTaskContext ctx, RecordDescriptor recordDesc, int partition, int numPartitions,
            ActivityId activityId, IBinaryComparatorFactory[] comparatorFactories, int[] sortFields) {
        super();
        this.minHeap = new PriorityQueue<>(numPartitions, buildHeapComparator());
        this.numPartitions = numPartitions;
        this.cursors = new PartitionCursor[numPartitions];
        this.ctx = ctx;
        this.comparatorFactories = comparatorFactories;
        inputDone = new boolean[numPartitions];
        this.comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; i++) {
            this.comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        this.sortFields = sortFields;
        this.recordDesc = recordDesc;

    }

    private Comparator<HeapEntry> buildHeapComparator() {
        return (e1, e2) -> {
            ITupleReference t1 = e1.getTuple();
            ITupleReference t2 = e2.getTuple();

            for (int i = 0; i < sortFields.length; i++) {
                int fIdx = sortFields[i];

                if (fIdx >= t1.getFieldCount() || fIdx >= t2.getFieldCount()) {
                    throw new RuntimeException("Invalid sort field index: " + fIdx + ", t1 fieldCount="
                            + t1.getFieldCount() + ", t2 fieldCount=" + t2.getFieldCount());
                }

                byte[] b1 = t1.getFieldData(fIdx);
                int s1 = t1.getFieldStart(fIdx);
                int l1 = t1.getFieldLength(fIdx);

                byte[] b2 = t2.getFieldData(fIdx);
                int s2 = t2.getFieldStart(fIdx);
                int l2 = t2.getFieldLength(fIdx);

                int cmp = 0;
                try {
                    cmp = comparators[i].compare(b1, s1, l1, b2, s2, l2);
                } catch (HyracksDataException e) {
                    throw new RuntimeException(e);
                }
                if (cmp != 0) {
                    System.out.println("Key value has changed");
                    return cmp;
                }
            }

            return 0; // all keys equal
        };
    }

    @Override
    public void initialize() throws HyracksDataException {
        try {
            outputFrame = new VSizeFrame(ctx);
            appender = new FrameTupleAppender();
            appender.reset(outputFrame, true);
            writer.open();

            // Start merge loop in a thread
            Thread mergeThread = new Thread(() -> {
                try {
                    runMergeLoop();
                } catch (Throwable th) {
                    th.printStackTrace();
                    throw new RuntimeException(th);
                }
            });
            mergeThread.start();

        } catch (Throwable th) {
            writer.fail();
            throw HyracksDataException.create(th);
        }
    }

    private void runMergeLoop() throws HyracksDataException {
        if (writer == null) {
            throw new HyracksDataException("Writer is null in runMergeLoop()");
        }
        while (true) {
            HeapEntry entry;
            synchronized (lock) {
                while (minHeap.size() < (numPartitions - numCompleted)) {
                    try {
                        lock.wait(); // wait for all inputs
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }

                if (minHeap.isEmpty() && numCompleted == numPartitions) {
                    break;
                } // all inputs done

                entry = minHeap.poll();
            }

            int src = entry.getSource();

            PartitionCursor cursor = cursors[src];
            FrameTupleAccessor accessor = cursor.accessor;
            int tIndex = cursor.tupleIndex;

            // Append to output
            if (tIndex < 0 || tIndex >= accessor.getTupleCount()) {
                throw new HyracksDataException("Invalid tuple index: " + tIndex);
            }
            if (!appender.append(accessor, tIndex)) {

                if (appender.getTupleCount() > 0) {
                    flushFrame(outputFrame.getBuffer(), writer);
                    appender.reset(outputFrame, true);
                    appender.append(accessor, tIndex);
                }
            }

            synchronized (lock) {
                if (cursor.hasMore()) {
                    cursor.advance();
                    FrameTupleReference tupleCopy = new FrameTupleReference();
                    tupleCopy.reset(cursor.accessor, cursor.tupleIndex);
                    minHeap.add(new HeapEntry(src, tupleCopy));

                }
            }
        }

        if (appender.getTupleCount() > 0) {
            flushFrame(outputFrame.getBuffer(), writer);
            appender.reset(outputFrame, true);
        }
        writer.close();
    }

    @Override
    public void deinitialize() throws HyracksDataException {
        if (appender != null && appender.getTupleCount() > 0) {
            flushFrame(outputFrame.getBuffer(), writer);
            appender.reset(outputFrame, true);
        }
        writer.close();

    }

    @Override
    public int getInputArity() {
        return numPartitions;
    }

    @Override
    public IFrameWriter getInputFrameWriter(int inputIndex) {
        return new IFrameWriter() {

            @Override
            public void open() {
            }

            @Override
            public void nextFrame(ByteBuffer frame) throws HyracksDataException {
                synchronized (lock) {
                    PartitionCursor cursor = cursors[inputIndex];

                    if (cursor == null) {
                        cursor = new PartitionCursor(recordDesc);
                        cursors[inputIndex] = cursor;
                    }

                    cursor.updateFrame(frame);

                    if (!cursor.isEmpty()) {
                        // Push the first tuple from this partition into the heap
                        FrameTupleReference tupleCopy = new FrameTupleReference();
                        tupleCopy.reset(cursor.accessor, cursor.tupleIndex);
                        HeapEntry entry = new HeapEntry(inputIndex, tupleCopy);
                        minHeap.add(entry);
                    }

                    // Wake up merge loop if all inputs have arrived
                    if (minHeap.size() == numPartitions - numCompleted) {
                        lock.notifyAll();
                    }
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void fail() throws HyracksDataException {

            }

            @Override
            public void close() throws HyracksDataException {
                synchronized (lock) {
                    if (!inputDone[inputIndex]) {
                        inputDone[inputIndex] = true;
                        numCompleted++;
                    }

                    if (numCompleted == numPartitions && minHeap.isEmpty()) {
                        lock.notifyAll(); // Trigger merge loop to exit
                    }
                }
                // Optional: signal that this input is done
            }
        };
    }

    private static class HeapEntry {
        private final int source;
        private final ITupleReference tuple;

        public HeapEntry(int inputIndex, ITupleReference tuple) {
            this.source = inputIndex;
            this.tuple = tuple;
        }

        public int getSource() {
            return source;
        }

        public ITupleReference getTuple() {
            return tuple;
        }
        //public Object getKey() { return key; }
    }

    private static class PartitionCursor {
        private final FrameTupleAccessor accessor;
        private final FrameTupleReference tupleRef; // mutable reference
        private ByteBuffer currentFrame;
        private int tupleIndex;

        public PartitionCursor(RecordDescriptor recordDesc) {
            this.accessor = new FrameTupleAccessor(recordDesc);
            this.tupleRef = new FrameTupleReference();
        }

        public void updateFrame(ByteBuffer frame) {
            this.currentFrame = frame;
            this.accessor.reset(frame);
            this.tupleIndex = 0;
            tupleRef.reset(accessor, tupleIndex);
        }

        public boolean hasMore() {
            return tupleIndex + 1 < accessor.getTupleCount();
        }

        public ITupleReference currentTuple() {
            return tupleRef;
        }

        public void advance() {
            tupleIndex++;
            tupleRef.reset(accessor, tupleIndex);
        }

        public boolean isEmpty() {
            return accessor.getTupleCount() == 0;
        }
    }
}
