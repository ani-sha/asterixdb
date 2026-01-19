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
package org.apache.hyracks.dataflow.std.group.sort;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.preclustered.PreclusteredGroupWriter;
import org.apache.hyracks.dataflow.std.sort.AbstractExternalSortRunMerger;

/**
 * Group-by aggregation is pushed into multi-pass merge of external sort.
 *
 * @author yingyib
 */
public class ExternalSortGroupByRunMerger extends AbstractExternalSortRunMerger {

    private final RecordDescriptor inputRecordDesc;
    private final RecordDescriptor partialAggRecordDesc;
    private final RecordDescriptor outRecordDesc;
    private final int[] groupFields;
    private final IAggregatorDescriptorFactory mergeAggregatorFactory;
    private final IAggregatorDescriptorFactory partialAggregatorFactory;
    private final boolean localSide;
    private final int[] mergeSortFields;
    private final int[] mergeGroupFields;
    private final IBinaryComparator[] groupByComparators;
    private boolean isGlobalGBY;
    private static final Path HYBRID_DIR = Paths.get("results", "HybridExecution");
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public ExternalSortGroupByRunMerger(IHyracksTaskContext ctx, List<GeneratedRunFileReader> runs, int[] sortFields,
            RecordDescriptor inRecordDesc, RecordDescriptor partialAggRecordDesc, RecordDescriptor outRecordDesc,
            int framesLimit, int[] groupFields, INormalizedKeyComputer nmk, IBinaryComparator[] comparators,
            IAggregatorDescriptorFactory partialAggregatorFactory, IAggregatorDescriptorFactory aggregatorFactory,
            boolean localStage, boolean isGlobalGBY) throws IOException {
        super(ctx, runs, comparators, nmk, partialAggRecordDesc, framesLimit);
        this.inputRecordDesc = inRecordDesc;
        this.partialAggRecordDesc = partialAggRecordDesc;
        this.outRecordDesc = outRecordDesc;
        this.groupFields = groupFields;
        this.mergeAggregatorFactory = aggregatorFactory;
        this.partialAggregatorFactory = partialAggregatorFactory;
        this.localSide = localStage;
        this.isGlobalGBY = isGlobalGBY;
        maybeSendB2ISignalAndWait(isGlobalGBY);



        //create merge sort fields
        int numSortFields = sortFields.length;
        mergeSortFields = new int[numSortFields];
        for (int i = 0; i < numSortFields; i++) {
            mergeSortFields[i] = i;
        }

        //create merge group fields
        int numGroupFields = groupFields.length;
        mergeGroupFields = new int[numGroupFields];
        for (int i = 0; i < numGroupFields; i++) {
            mergeGroupFields[i] = i;
        }

        //setup comparators for grouping
        groupByComparators = new IBinaryComparator[Math.min(mergeGroupFields.length, comparators.length)];
        for (int i = 0; i < groupByComparators.length; i++) {
            groupByComparators[i] = comparators[i];
        }
    }

    public ExternalSortGroupByRunMerger(IHyracksTaskContext ctx, List<GeneratedRunFileReader> runs, int[] sortFields,
            RecordDescriptor inRecordDesc, RecordDescriptor partialAggRecordDesc, RecordDescriptor outRecordDesc,
            int framesLimit, int[] groupFields, INormalizedKeyComputer nmk, IBinaryComparator[] comparators,
            IAggregatorDescriptorFactory partialAggregatorFactory, IAggregatorDescriptorFactory aggregatorFactory,
            boolean localStage) throws IOException {
        super(ctx, runs, comparators, nmk, partialAggRecordDesc, framesLimit);
        this.inputRecordDesc = inRecordDesc;
        this.partialAggRecordDesc = partialAggRecordDesc;
        this.outRecordDesc = outRecordDesc;
        this.groupFields = groupFields;
        this.mergeAggregatorFactory = aggregatorFactory;
        this.partialAggregatorFactory = partialAggregatorFactory;
        this.localSide = localStage;

        maybeSendB2ISignalAndWait(isGlobalGBY);
        //create merge sort fields





        //create merge sort fields
        int numSortFields = sortFields.length;
        mergeSortFields = new int[numSortFields];
        for (int i = 0; i < numSortFields; i++) {
            mergeSortFields[i] = i;
        }

        //create merge group fields
        int numGroupFields = groupFields.length;
        mergeGroupFields = new int[numGroupFields];
        for (int i = 0; i < numGroupFields; i++) {
            mergeGroupFields[i] = i;
        }

        //setup comparators for grouping
        groupByComparators = new IBinaryComparator[Math.min(mergeGroupFields.length, comparators.length)];
        for (int i = 0; i < groupByComparators.length; i++) {
            groupByComparators[i] = comparators[i];
        }
    }

    private static void maybeSendB2ISignalAndWait(boolean shouldSignal) throws IOException {
        if (!shouldSignal) {
            return;
        }

        Files.createDirectories(HYBRID_DIR);

        final Path b2iPath = HYBRID_DIR.resolve("B2ISignal");
        final Instant startTime = Instant.now();

        System.out.println("This is when a B2I Signal goes out at " + LocalDateTime.now().format(TS_FMT));
        System.out.println("Sent signal to " + b2iPath.toAbsolutePath()
                + " to stop interactive processing at " + LocalDateTime.now().format(TS_FMT));

        // Match ResultWriter's check for B2ISignal: "Yes."
        Files.writeString(b2iPath, "Yes.", StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        final int requiredAcks = 1; // your existing code breaks at >= 1, so keep it consistent
        final Set<Path> confirmedFiles = new HashSet<>();

        while (true) {
            try (Stream<Path> files = Files.list(HYBRID_DIR)
                    .filter(p -> p.getFileName().toString().startsWith("I2BSignal"))) {

                files.forEach(path -> {
                    if (confirmedFiles.contains(path)) {
                        return;
                    }
                    try {
                        String content = Files.readString(path).trim();
                        // ResultWriter writes "Yes" (no dot) to I2BSignal
                        if ("Yes".equals(content)) {
                            confirmedFiles.add(path);
                            System.out.println("Received ok from: " + path.getFileName());
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }

            if (confirmedFiles.size() >= requiredAcks) {
                System.out.println("Received " + confirmedFiles.size()
                        + " 'Yes' signal(s) from interactive plan.");
                break;
            }

            if (Duration.between(startTime, Instant.now()).getSeconds() > 180) {
                throw new IOException("Timeout: Did not receive " + requiredAcks
                        + " 'Yes' signal(s) within 3 minutes.");
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for I2BSignal", ie);
            }
        }
    }

    @Override
    public IFrameWriter prepareSkipMergingFinalResultWriter(IFrameWriter nextWriter) throws HyracksDataException {
        IAggregatorDescriptorFactory aggregatorFactory = localSide ? partialAggregatorFactory : mergeAggregatorFactory;
        return new PreclusteredGroupWriter(ctx, groupFields, groupByComparators, aggregatorFactory, inputRecordDesc,
                outRecordDesc, nextWriter, false);
    }

    @Override
    protected RunFileWriter prepareIntermediateMergeRunFile() throws HyracksDataException {
        FileReference newRun = ctx.createManagedWorkspaceFile(ExternalSortGroupByRunMerger.class.getSimpleName());
        return new RunFileWriter(newRun, ctx.getIoManager());
    }

    @Override
    protected IFrameWriter prepareIntermediateMergeResultWriter(RunFileWriter mergeFileWriter)
            throws HyracksDataException {
        IAggregatorDescriptorFactory aggregatorFactory = localSide ? mergeAggregatorFactory : partialAggregatorFactory;
        return new PreclusteredGroupWriter(ctx, mergeGroupFields, groupByComparators, aggregatorFactory,
                partialAggRecordDesc, partialAggRecordDesc, mergeFileWriter, true);
    }

    @Override
    public IFrameWriter prepareFinalMergeResultWriter(IFrameWriter nextWriter) throws HyracksDataException {
        return new PreclusteredGroupWriter(ctx, mergeGroupFields, groupByComparators, mergeAggregatorFactory,
                partialAggRecordDesc, outRecordDesc, nextWriter, false);
    }

    @Override
    protected int[] getSortFields() {
        return mergeSortFields;
    }
}
