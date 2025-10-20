package org.apache.hyracks.dataflow.std.base;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;

public class EagerMergeOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private final RecordDescriptor recordDesc;

    private final IBinaryComparatorFactory[] comparatorFactories;
    private final int[] sortFields;

    public EagerMergeOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recordDesc,
            IBinaryComparatorFactory[] comparatorFactories, int[] sortFields) {
        super(spec, 1, 1);
        this.recordDesc = recordDesc;
        this.comparatorFactories = comparatorFactories;
        this.sortFields = sortFields;
        this.outRecDescs[0] = recordDesc;

    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new EagerMerger(ctx, recordDesc, partition, nPartitions, getActivityId(), comparatorFactories,
                sortFields);
    }
}

//public class EagerMergeOperatorDescriptor extends AbstractOperatorDescriptor {
//    private static final long serialVersionUID = 1L;
//    private final int inputArity;
//    private final RecordDescriptor recordDesc;
//
//    public EagerMergeOperatorDescriptor(IOperatorDescriptorRegistry spec,
//                                        RecordDescriptor recordDesc,
//                                        int inputArity) {
//        super(spec, inputArity, 1);
//        this.inputArity = inputArity;
//        this.recordDesc = recordDesc;
//        for (int i = 0; i < inputArity; i++) {
//            recordDescriptors[i] = recordDesc;
//        }
//        recordDescriptors[inputArity] = recordDesc; // for output
//    }
//
//    @Override
//    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
//                                                   IRecordDescriptorProvider recordDescProvider,
//                                                   int partition,
//                                                   int nPartitions) {
//        return new EagerMergeOperatorNodePushable(ctx, inputArity, recordDesc, recordDescProvider.getInputRecordDescriptor(partition, 0));
//    }
//}
//
//class EagerMergeOperatorNodePushable implements IOperatorNodePushable {
//    private final IHyracksTaskContext ctx;
//    private final int inputArity;
//    private final BlockingQueue<ByteBuffer>[] inputQueues;
//    private final Object[] maxKeys;
//    private final AtomicBoolean[] done;
//    private final AtomicBoolean allDone = new AtomicBoolean(false);
//    private final PriorityQueue<HeadTuple> heap;
//    private final Queue<ITupleReference>[] bufferedTuples;
//    private IFrameWriter outputWriter;
//    private final Thread mergeThread;
//    private final RecordDescriptor recordDesc;
//    private final FrameTupleAccessor[] accessors;
//    private FrameTupleAppender appender;
//    private ByteBuffer outputBuffer;
//
//    public EagerMergeOperatorNodePushable(IHyracksTaskContext ctx, int inputArity, RecordDescriptor recordDesc, RecordDescriptor inputRecordDesc) {
//        this.ctx = ctx;
//        this.inputArity = inputArity;
//        this.recordDesc = recordDesc;
//        this.inputQueues = new LinkedBlockingQueue[inputArity];
//        this.maxKeys = new Object[inputArity];
//        this.done = new AtomicBoolean[inputArity];
//        this.heap = new PriorityQueue<>(Comparator.comparing(HeadTuple::getKey));
//        this.bufferedTuples = new LinkedList[inputArity];
//        this.accessors = new FrameTupleAccessor[inputArity];
//
//        for (int i = 0; i < inputArity; i++) {
//            inputQueues[i] = new LinkedBlockingQueue<>();
//            done[i] = new AtomicBoolean(false);
//            bufferedTuples[i] = new LinkedList<>();
//            accessors[i] = new FrameTupleAccessor(ctx.getFrameSize(), inputRecordDesc);
//        }
//
//        this.mergeThread = new Thread(this::runMergeLoop);
//    }
//
//    @Override
//    public IFrameWriter getInputFrameWriter(int index) {
//        return new IFrameWriter() {
//            @Override public void open() {}
//            @Override public void close() { done[index].set(true); }
//            @Override public void fail() {}
//            @Override public void flush() {}
//            @Override
//            public void nextFrame(ByteBuffer frame) throws HyracksDataException {
//                ByteBuffer copy = ByteBuffer.allocate(frame.remaining());
//                copy.put(frame);
//                copy.flip();
//                inputQueues[index].offer(copy);
//            }
//        };
//    }
//
//    private void runMergeLoop() {
//        try {
//            outputBuffer = ctx.allocateFrame();
//            appender = new FrameTupleAppender(ctx.getFrameSize());
//            appender.reset(outputBuffer, true);
//            outputWriter.open();
//
//            while (!allDone.get()) {
//                for (int i = 0; i < inputArity; i++) {
//                    ByteBuffer frame = inputQueues[i].poll();
//                    if (frame != null) {
//                        processFrame(i, frame);
//                    }
//                }
//
//                Object globalMin = computeGlobalMinKey();
//                emitSafeTuples(globalMin);
//
//                if (areAllInputsClosed() && allBuffersEmpty()) {
//                    allDone.set(true);
//                    flushRemaining();
//                    outputWriter.close();
//                }
//                Thread.sleep(2);
//            }
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    private void processFrame(int inputIndex, ByteBuffer frame) {
//        FrameTupleAccessor accessor = accessors[inputIndex];
//        accessor.reset(frame);
//        int count = accessor.getTupleCount();
//        for (int t = 0; t < count; t++) {
//            ITupleReference tuple = accessor.createTupleReference(t);
//            bufferedTuples[inputIndex].offer(tuple);
//        }
//        ITupleReference last = accessor.createTupleReference(count - 1);
//        maxKeys[inputIndex] = extractKey(last);
//
//        ITupleReference head = bufferedTuples[inputIndex].peek();
//        if (head != null) {
//            heap.offer(new HeadTuple(inputIndex, head, extractKey(head)));
//        }
//    }
//
//    private void emitSafeTuples(Object threshold) throws HyracksDataException {
//        while (!heap.isEmpty() && compareKeys(heap.peek().getKey(), threshold) <= 0) {
//            HeadTuple head = heap.poll();
//            int src = head.getSource();
//            ITupleReference tuple = bufferedTuples[src].poll();
//            if (!appender.append(tuple, recordDesc)) {
//                outputWriter.nextFrame(outputBuffer);
//                outputBuffer = ctx.allocateFrame();
//                appender.reset(outputBuffer, true);
//                appender.append(tuple, recordDesc);
//            }
//            ITupleReference next = bufferedTuples[src].peek();
//            if (next != null) {
//                heap.offer(new HeadTuple(src, next, extractKey(next)));
//            }
//        }
//    }
//
//    private void flushRemaining() throws HyracksDataException {
//        if (appender.getTupleCount() > 0) {
//            outputWriter.nextFrame(outputBuffer);
//        }
//    }
//
//    private Object computeGlobalMinKey() {
//        Object min = null;
//        for (Object k : maxKeys) {
//            if (k == null) continue;
//            if (min == null || compareKeys(k, min) < 0) {
//                min = k;
//            }
//        }
//        return min;
//    }
//
//    private boolean areAllInputsClosed() {
//        for (AtomicBoolean b : done) {
//            if (!b.get()) return false;
//        }
//        return true;
//    }
//
//    private boolean allBuffersEmpty() {
//        for (Queue<?> q : bufferedTuples) {
//            if (!q.isEmpty()) return false;
//        }
//        return true;
//    }
//
//    private Object extractKey(ITupleReference tuple) {
//        // Implement: deserialize the key field from the tuple
//        return null; // placeholder
//    }
//
//    private int compareKeys(Object k1, Object k2) {
//        // Implement: use proper comparator for key type
//        return ((Comparable) k1).compareTo(k2);
//    }
//
//    @Override public void open() { mergeThread.start(); }
//    @Override public void close() {}
//    @Override public void fail() {}
//    @Override public void flush() {}
//
//    private static class HeadTuple {
//        private final int source;
//        private final ITupleReference tuple;
//        private final Object key;
//
//        public HeadTuple(int source, ITupleReference tuple, Object key) {
//            this.source = source;
//            this.tuple = tuple;
//            this.key = key;
//        }
//
//        public int getSource() { return source; }
//        public ITupleReference getTuple() { return tuple; }
//        public Object getKey() { return key; }
//    }
//}
