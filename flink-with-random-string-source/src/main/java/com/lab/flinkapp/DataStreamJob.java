/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lab.flinkapp;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        env.setParallelism(1);

        // 1. Instantiate the modern, custom Source
        RandomStringSource source = new RandomStringSource(5);

        // 2. Use env.fromSource() to initialize the DataStream
        DataStream<String> textStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Canonical Random String Generator Source"
        );

        // 3. Simple WordCount Transformation
        DataStream<Tuple2<String, Integer>> counts = textStream
                .flatMap(new Tokenizer())
                .keyBy(value -> value.f0)
                .sum(1);

        // 4. Sink Output
        counts.print().name("Word Count Result (Continuous");

        env.execute("Canonical Flink Source Job");
    }

    /**
     * Standard Flink FlatMap function for WordCount.
     */
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String token = value.trim().toLowerCase();
            if (!token.isEmpty()) {
                out.collect(new Tuple2<>(token, 1));
            }
        }
    }

    // =======================================================================
    // LATEST CANONICAL FLINK SOURCE API COMPONENTS
    // =======================================================================

    /**
     * 1. The main Source connector, implementing the generic Source interface.
     * <String, RandomSplit, Long> -> <Output Data Type, Split Type, Checkpoint State Type>
     */
    public static class RandomStringSource implements Source<String, RandomSplit, Long> {
        private final int maxWordLength;

        public RandomStringSource(int maxWordLength) {
            this.maxWordLength = maxWordLength;
        }

        // --- MANDATORY IMPLEMENTATION FOR CANONICAL SOURCE ---
        @Override
        public Boundedness getBoundedness() {
            // Explicitly declares that this is a continuous, unbounded source (stream).
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }
        // -----------------------------------------------------

        @Override
        public SplitEnumerator<RandomSplit, Long> createEnumerator(
                SplitEnumeratorContext<RandomSplit> enumContext) {
            return new RandomSplitEnumerator(enumContext);
        }

        @Override
        public SplitEnumerator<RandomSplit, Long> restoreEnumerator(
                SplitEnumeratorContext<RandomSplit> enumContext, Long checkpoint) {
            return new RandomSplitEnumerator(enumContext, checkpoint);
        }

        @Override
        public SimpleVersionedSerializer<RandomSplit> getSplitSerializer() {
            return new RandomSplitSerializer();
        }

        @Override
        public SimpleVersionedSerializer<Long> getEnumeratorCheckpointSerializer() {
            return new LongCheckpointSerializer();
        }

        @Override
        public SourceReader<String, RandomSplit> createReader(
                SourceReaderContext readerContext) {
            return new RandomStringSourceReader(readerContext, maxWordLength);
        }
    }

    /**
     * 2. The Split implementation (a placeholder for this single continuous stream).
     */
    public static class RandomSplit implements SourceSplit {
        private final String id;

        public RandomSplit(String id) {
            this.id = id;
        }

        @Override
        public String splitId() {
            return id;
        }
    }

    /**
     * 3. The SplitEnumerator runs on the JobManager to assign splits.
     */
    public static class RandomSplitEnumerator implements SplitEnumerator<RandomSplit, Long> {

        private final SplitEnumeratorContext<RandomSplit> context;
        private boolean isInitialSplitAssigned = false;

        public RandomSplitEnumerator(SplitEnumeratorContext<RandomSplit> context) {
            this.context = context;
        }

        public RandomSplitEnumerator(SplitEnumeratorContext<RandomSplit> context, Long lastCheckpoint) {
            this.context = context;
        }

        @Override
        public void start() {
            // Since we only ever assign one split, we use a list creator inside callAsync
            final List<RandomSplit> splits = Collections.singletonList(new RandomSplit("random-stream-0"));

            context.callAsync(
                    () -> splits,
                    (s, error) -> {
                        if (error != null) return;
                        if (!context.registeredReaders().isEmpty()) {
                            int subtaskId = context.registeredReaders().keySet().iterator().next();

                            // FIX: Use the single-split constructor for SplitsAssignment,
                            // as we know 's' contains exactly one element.
                            context.assignSplits(new SplitsAssignment<>(s.get(0), subtaskId));
                            isInitialSplitAssigned = true;
                        }
                    }
            );
        }

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) {}

        @Override
        public void addSplitsBack(List<RandomSplit> splits, int subtaskId) {
            context.assignSplits(new SplitsAssignment<>(splits.get(0), subtaskId));
        }

        @Override
        public void addReader(int subtaskId) {
            if (!isInitialSplitAssigned) {
                start();
            }
        }

        @Override
        public Long snapshotState(long checkpointId) {
            return System.currentTimeMillis();
        }

        @Override
        public void close() {}
    }

    /**
     * 4. The SourceReader runs on the TaskManager and continuously generates data.
     */
    public static class RandomStringSourceReader implements SourceReader<String, RandomSplit> {

        private RandomSplit assignedSplit = null;
        private final Random random = new Random();
        private final char[] characters = "abcdefghijklmnopqrstuvwxyz".toCharArray();
        private final int maxWordLength;

        public RandomStringSourceReader(SourceReaderContext readerContext, int maxWordLength) {
            this.maxWordLength = maxWordLength;
        }

        @Override
        public void start() {}

        /**
         * Flink calls this repeatedly to get the next record.
         */
        @Override
        public InputStatus pollNext(ReaderOutput<String> output) throws Exception {
            if (assignedSplit == null) {
                return InputStatus.NOTHING_AVAILABLE;
            }

            // --- Random String Generation ---
            int length = random.nextInt(maxWordLength) + 1;
            StringBuilder sb = new StringBuilder(length);
            for (int i = 0; i < length; i++) {
                sb.append(characters[random.nextInt(characters.length)]);
            }
            String randomWord = sb.toString();

            // Emit the generated word
            output.collect(randomWord, System.currentTimeMillis());

            // Throttle the stream rate
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return InputStatus.END_OF_INPUT;
            }

            // UNBOUNDED Source: We always return NOTHING_AVAILABLE
            return InputStatus.NOTHING_AVAILABLE;
        }

        @Override
        public List<RandomSplit> snapshotState(long checkpointId) {
            if (assignedSplit != null) {
                return Collections.singletonList(assignedSplit);
            }
            return Collections.emptyList();
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void addSplits(List<RandomSplit> splits) {
            if (!splits.isEmpty()) {
                assignedSplit = splits.get(0);
            }
        }

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public void close() {}
    }

    // =======================================================================
    // SERIALIZERS (Required for checkpointing)
    // =======================================================================

    public static class RandomSplitSerializer implements SimpleVersionedSerializer<RandomSplit> {
        @Override public int getVersion() { return 1; }
        @Override
        public byte[] serialize(RandomSplit split) throws IOException {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeUTF(split.splitId());
                return baos.toByteArray();
            }
        }
        @Override
        public RandomSplit deserialize(int version, byte[] serialized) throws IOException {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                 DataInputStream dis = new DataInputStream(bais)) {
                String id = dis.readUTF();
                return new RandomSplit(id);
            }
        }
    }

    public static class LongCheckpointSerializer implements SimpleVersionedSerializer<Long> {
        @Override public int getVersion() { return 1; }
        @Override
        public byte[] serialize(Long state) throws IOException {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeLong(state);
                return baos.toByteArray();
            }
        }
        @Override
        public Long deserialize(int version, byte[] serialized) throws IOException {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                 DataInputStream dis = new DataInputStream(bais)) {
                return dis.readLong();
            }
        }
    }
}
