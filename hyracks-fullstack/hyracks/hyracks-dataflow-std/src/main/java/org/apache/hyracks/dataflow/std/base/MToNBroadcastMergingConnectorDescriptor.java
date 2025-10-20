package org.apache.hyracks.dataflow.std.base;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.hyracks.api.comm.IFrameReader;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.IPartitionCollector;
import org.apache.hyracks.api.comm.IPartitionWriterFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.*;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.collectors.IPartitionBatchManager;
import org.apache.hyracks.dataflow.std.collectors.NonDeterministicPartitionBatchManager;
import org.apache.hyracks.dataflow.std.collectors.PartitionCollector;
import org.apache.hyracks.dataflow.std.collectors.SortMergeFrameReader;

public class MToNBroadcastMergingConnectorDescriptor extends AbstractMToNConnectorDescriptor {
    private final int[] sortFields;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final INormalizedKeyComputerFactory nkcFactory;

    public MToNBroadcastMergingConnectorDescriptor(IConnectorDescriptorRegistry spec, int[] sortFields,
            IBinaryComparatorFactory[] comparatorFactories, INormalizedKeyComputerFactory nkcFactory) {

        super(spec);
        this.sortFields = sortFields;
        this.comparatorFactories = comparatorFactories;
        this.nkcFactory = nkcFactory;
    }

    @Override
    public IFrameWriter createPartitioner(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            IPartitionWriterFactory edwFactory, int index, int nProducerPartitions, int nConsumerPartitions)
            throws HyracksDataException {
        final IFrameWriter[] epWriters = new IFrameWriter[nConsumerPartitions];
        final boolean[] isOpen = new boolean[nConsumerPartitions];
        for (int i = 0; i < nConsumerPartitions; ++i) {
            epWriters[i] = edwFactory.createFrameWriter(i);
        }
        return new IFrameWriter() {
            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                // Record the current position, instead of using buffer.mark().
                // The latter will be problematic because epWriters[i].nextFrame(buffer)
                // can flip or clear the buffer.
                int pos = buffer.position();
                for (int i = 0; i < epWriters.length; ++i) {
                    if (i != 0) {
                        buffer.position(pos);
                    }
                    epWriters[i].nextFrame(buffer);
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                HyracksDataException failException = null;
                for (int i = 0; i < epWriters.length; ++i) {
                    if (isOpen[i]) {
                        try {
                            epWriters[i].fail();
                        } catch (Throwable th) {
                            if (failException == null) {
                                failException = HyracksDataException.create(th);
                            } else {
                                failException.addSuppressed(th);
                            }
                        }
                    }
                }
                if (failException != null) {
                    throw failException;
                }
            }

            @Override
            public void close() throws HyracksDataException {
                HyracksDataException closeException = null;
                for (int i = 0; i < epWriters.length; ++i) {
                    if (isOpen[i]) {
                        try {
                            epWriters[i].close();
                        } catch (Throwable th) {
                            if (closeException == null) {
                                closeException = HyracksDataException.create(th);
                            } else {
                                closeException.addSuppressed(th);
                            }
                        }
                    }
                }
                if (closeException != null) {
                    throw closeException;
                }
            }

            @Override
            public void open() throws HyracksDataException {
                for (int i = 0; i < epWriters.length; ++i) {
                    isOpen[i] = true;
                    epWriters[i].open();
                }
            }

            @Override
            public void flush() throws HyracksDataException {
                for (IFrameWriter writer : epWriters) {
                    writer.flush();
                }
            }
        };
    }

    @Override
    public IPartitionCollector createPartitionCollector(IHyracksTaskContext ctx, RecordDescriptor recordDesc, int index,
            int nProducerPartitions, int nConsumerPartitions) throws HyracksDataException {
        IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        INormalizedKeyComputer nmkComputer = nkcFactory == null ? null : nkcFactory.createNormalizedKeyComputer();
        IPartitionBatchManager pbm = new NonDeterministicPartitionBatchManager(nProducerPartitions);
        IFrameReader sortMergeFrameReader = new SortMergeFrameReader(ctx, nProducerPartitions, nProducerPartitions,
                sortFields, comparators, nmkComputer, recordDesc, pbm);
        BitSet expectedPartitions = new BitSet();
        expectedPartitions.set(0, nProducerPartitions);
        return new PartitionCollector(ctx, getConnectorId(), index, expectedPartitions, sortMergeFrameReader, pbm);
    }
}
