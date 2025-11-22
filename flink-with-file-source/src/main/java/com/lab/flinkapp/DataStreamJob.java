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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

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

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token: tokens) {
                if (token.length() > 0) {
                    collector.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

	public static void main(String[] args) throws Exception {
        // 1. create a stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set parallelism to 1 for easier understanding of file reading order
        env.setParallelism(1);

        // 2. to create a DataStream from some source
        final String inputPath = "/Users/yangruiguo/Desktop/command.txt";

        // Create the FileSource using the builder pattern
        final FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(inputPath))
                .monitorContinuously(Duration.ofSeconds(5))
                .build();

        final DataStream<String> textStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "File Source"
        );

        // 3. to do something with the data stream (call some of the flink operators)
        // transformation (simple word count)
        final DataStream<Tuple2<String, Integer>> counts = textStream
                .flatMap(new Tokenizer()).keyBy(value -> value.f0).sum(1);

        // 4. the results from step 3 should be sent to a data sink (.print())
        counts.print().name("Word Count Result");

        // 5. execute
        env.execute(DataStreamJob.class.getName());
	}
}
