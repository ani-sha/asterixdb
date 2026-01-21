/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.control.nc.partitions;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executor;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.api.partitions.IPartition;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.control.common.job.PartitionState;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MaterializingPipelinedPartition implements IFrameWriter, IPartition {
    private static final Logger LOGGER = LogManager.getLogger();

    private static final long DEFAULT_CAPACITY = 1L << 29;

    private final IHyracksTaskContext ctx;
    private final Executor executor;
    private final PartitionManager manager;
    private final PartitionId pid;
    private final TaskAttemptId taId;
    private FileReference fRef;
    private RandomAccessFile raf;
    private FileChannel writeChannel;
    private long writePosition;
    private long readPosition;
    private long bufferedBytes;
    private final long capacity;
    private boolean eos;
    private boolean failed;
    protected boolean flushRequest;
    private boolean deallocated;
    private Thread dataConsumerThread;
    private IFileHandle writeHandle;
    private final IIOManager ioManager;


    public MaterializingPipelinedPartition(IHyracksTaskContext ctx, PartitionManager manager, PartitionId pid,
                                           TaskAttemptId taId, Executor executor) {
        this.ctx = ctx;
        this.executor = executor;
        this.ioManager = ctx.getIoManager();
        this.manager = manager;
        this.pid = pid;
        this.taId = taId;
        this.capacity = DEFAULT_CAPACITY;
    }

    @Override
    public IHyracksTaskContext getTaskContext() {
        return ctx;
    }

    @Override
    public synchronized void deallocate() {
        // Makes sure that the data consumer thread will not wait for anything further. Since the receiver side could
        // have be interrupted already, the data consumer thread can potentially hang on writer.nextFrame(...)
        // or writer.close(...).  Note that Task.abort(...) cannot interrupt the dataConsumerThread.
        // If the query runs successfully, the dataConsumer thread should have been completed by this time.
        if (dataConsumerThread != null) {
            dataConsumerThread.interrupt();
        }
        deallocated = true;
        notifyAll();
    }

    @Override
    public void writeTo(final IFrameWriter writer) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                Thread thread = Thread.currentThread();
                setDataConsumerThread(thread); // Sets the data consumer thread to the current thread.
                try {
                    thread.setName(MaterializingPipelinedPartition.this.getClass().getSimpleName() + " " + pid);
                    FileReference fRefCopy;
                    synchronized (MaterializingPipelinedPartition.this) {
                        while (fRef == null && !eos && !failed) {
                            MaterializingPipelinedPartition.this.wait();
                        }
                        fRefCopy = fRef;
                    }
                    writer.open();
                    FileChannel readChannel = fRefCopy == null ? null
                            : FileChannel.open(fRefCopy.getFile().toPath(), StandardOpenOption.READ);
                    try {
                        if (readChannel == null) {
                            // Either fail() is called or close() is called with 0 tuples coming in.
                            return;
                        }
                        synchronized (MaterializingPipelinedPartition.this) {
                            if (deallocated) {
                                return;
                            }
                        }
                        ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES);
                        ByteBuffer frameBuffer = ctx.allocateFrame();
                        boolean done = false;
                        while (!done) {
                            ByteBuffer outFrame = null;
                            boolean flush = false;
                            boolean fail;
                            int frameLength = 0;
                            synchronized (MaterializingPipelinedPartition.this) {
                                while (bufferedBytes < Integer.BYTES && !eos && !failed && !deallocated) {
                                    MaterializingPipelinedPartition.this.wait();
                                }
                                flush = flushRequest;
                                flushRequest = false;
                                fail = failed || deallocated;
                                if (fail) {
                                    // Leave the loop and fail the writer outside the synchronized block.
                                } else if (bufferedBytes < Integer.BYTES && eos) {
                                    done = true;
                                } else {
                                    long frameStart = readPosition;
                                    readFully(readChannel, frameStart, lengthBuffer, Integer.BYTES);
                                    frameLength = lengthBuffer.getInt();
                                    while (bufferedBytes < Integer.BYTES + frameLength && !eos && !failed
                                            && !deallocated) {
                                        MaterializingPipelinedPartition.this.wait();
                                    }
                                    fail = failed || deallocated;
                                    if (!fail) {
                                        if (frameLength > frameBuffer.capacity()) {
                                            frameBuffer = ctx.allocateFrame(frameLength);
                                        }
                                        readFrame(readChannel, frameBuffer, frameStart, frameLength);
                                        readPosition = advance(frameStart, Integer.BYTES + frameLength);
                                        bufferedBytes -= Integer.BYTES + frameLength;
                                        outFrame = frameBuffer.duplicate();
                                        MaterializingPipelinedPartition.this.notifyAll();
                                    }
                                }
                            }
                            if (fail) {
                                writer.fail();
                                break;
                            }
                            if (outFrame != null) {
                                writer.nextFrame(outFrame);
                            } else if (done) {
                                break;
                            }
                            if (flush) {
                                writer.flush();
                            }
                        }
                    } catch (Exception e) {
                        writer.fail();
                        throw e;
                    } finally {
                        try {
                            writer.close();
                        } finally {
                            // Makes sure that the reader is always closed and the temp file is always deleted.
                            try {
                                if (readChannel != null) {
                                    readChannel.close();
                                }
                            } finally {
                                if (fRefCopy != null) {
                                    fRefCopy.delete();
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    LOGGER.log(ExceptionUtils.causedByInterrupt(e) ? Level.DEBUG : Level.WARN,
                            "Failure writing to a frame", e);
                } finally {
                    setDataConsumerThread(null); // Sets back the data consumer thread to null.
                }
            }
        });
    }

    @Override
    public boolean isReusable() {
        return true;
    }

    @Override
    public void open() throws HyracksDataException {
        writePosition = 0;
        readPosition = 0;
        bufferedBytes = 0;
        eos = false;
        failed = false;
        deallocated = false;
        flushRequest = false;
        fRef = null;
        raf = null;
        writeChannel = null;
        manager.registerPartition(pid, ctx.getJobletContext().getJobId().getCcId(), taId, this, PartitionState.STARTED,
                false);
    }

    private void checkOrCreateFile() throws HyracksDataException {
        if (fRef == null) {
            String fileName = pid.toString().replace(":", "$") + ".waf";
            File ramdiskDir = new File("/dev/shm");

            // Confirm ramdiskDir exists
            if (!ramdiskDir.exists() || !ramdiskDir.isDirectory()) {
                throw new HyracksDataException("RAM disk directory does not exist or is not a directory: " + ramdiskDir);
            }

            // Create the actual file in the ramdisk
            File rawFile = new File(ramdiskDir, fileName);
            try {
                if (!rawFile.exists() && !rawFile.createNewFile()) {
                    throw new HyracksDataException("Failed to create materialization file: " + rawFile.getAbsolutePath());
                }

                // Preallocate the file to fixed capacity (in bytes)
                raf = new RandomAccessFile(rawFile, "rw");
                raf.setLength(capacity);  // capacity must be defined elsewhere
                writeChannel = raf.getChannel();

                // Wrap the file in dummy I/O handle and FileReference
                IODeviceHandle dummyHandle = new IODeviceHandle(ramdiskDir, ramdiskDir.getAbsolutePath());
                fRef = new FileReference(dummyHandle, fileName);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }

            synchronized (this) {
                notifyAll();  // Notify any thread waiting for fRef to be initialized
            }
        }
    }


    @Override
    public synchronized void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        checkOrCreateFile();
        int frameLength = buffer.remaining();
        int totalLength = Integer.BYTES + frameLength;
        if (totalLength > capacity) {
            throw new HyracksDataException("Frame size exceeds partition capacity");
        }
        while (capacity - bufferedBytes < totalLength && !failed && !deallocated) {
            try {
                wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw HyracksDataException.create(e);
            }
        }
        if (failed) {
            throw new HyracksDataException("Partition failed");
        }
        if (deallocated) {
            throw new HyracksDataException("Partition deallocated");
        }
        ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES);
        lengthBuffer.putInt(frameLength);
        lengthBuffer.flip();
        ByteBuffer frameData = buffer.duplicate();
        frameData.position(buffer.position());
        frameData.limit(buffer.limit());
        try {
            long nextPosition = writeBuffer(writePosition, lengthBuffer);
            writePosition = writeBuffer(nextPosition, frameData);
            bufferedBytes += totalLength;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        notifyAll();
    }

    @Override
    public synchronized void fail() throws HyracksDataException {
        failed = true;
        notifyAll();
    }

    @Override
    public void close() throws HyracksDataException {
        synchronized (this) {
            eos = true;
            if (writeChannel != null) {
                try {
                    writeChannel.close();
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                } finally {
                    writeChannel = null;
                }
            }
            if (raf != null) {
                try {
                    raf.close();
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                } finally {
                    raf = null;
                }
            }
            notifyAll();
        }
    }

    @Override
    public synchronized void flush() throws HyracksDataException {
        flushRequest = true;
        notifyAll();
    }

    // Sets the data consumer thread.
    private synchronized void setDataConsumerThread(Thread thread) {
        dataConsumerThread = thread;
    }

    private long advance(long position, long delta) {
        long newPosition = position + delta;
        if (newPosition >= capacity) {
            newPosition %= capacity;
        }
        return newPosition;
    }

    private long writeBuffer(long position, ByteBuffer src) throws IOException {
        ByteBuffer buffer = src.duplicate();
        long currentPosition = position;
        while (buffer.hasRemaining()) {
            int chunk = (int) Math.min(buffer.remaining(), capacity - currentPosition);
            int oldLimit = buffer.limit();
            buffer.limit(buffer.position() + chunk);
            long offset = currentPosition;
            while (buffer.hasRemaining()) {
                offset += writeChannel.write(buffer, offset);
            }
            buffer.limit(oldLimit);
            currentPosition = advance(currentPosition, chunk);
        }
        return currentPosition;
    }

    private void readFully(FileChannel channel, long position, ByteBuffer dest, int length)
            throws IOException, HyracksDataException {
        if (dest.capacity() < length) {
            throw new HyracksDataException("Destination buffer too small to read frame");
        }
        dest.clear();
        dest.limit(length);
        long currentPosition = position;
        int remaining = length;
        while (remaining > 0) {
            int chunk = (int) Math.min(remaining, capacity - currentPosition);
            ByteBuffer chunkBuffer = dest.duplicate();
            chunkBuffer.position(dest.position());
            chunkBuffer.limit(dest.position() + chunk);
            long offset = currentPosition;
            while (chunkBuffer.hasRemaining()) {
                int bytesRead = channel.read(chunkBuffer, offset);
                if (bytesRead < 0) {
                    throw new EOFException("Premature end of partition file");
                }
                offset += bytesRead;
            }
            dest.position(dest.position() + chunk);
            remaining -= chunk;
            currentPosition = advance(currentPosition, chunk);
        }
        dest.flip();
    }

    private void readFrame(FileChannel channel, ByteBuffer frameBuffer, long frameStart, int frameLength)
            throws IOException, HyracksDataException {
        readFully(channel, advance(frameStart, Integer.BYTES), frameBuffer, frameLength);
    }

}
