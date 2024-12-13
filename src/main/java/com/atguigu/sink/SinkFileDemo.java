package com.atguigu.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

public class SinkFileDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setBootstrapServers("10.15.8.145:9092")
                .setGroupId("leon_li")
                .setTopics("paperless_csv_test_20241120")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStreamSource<String> dataGen = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kasource");

//        dataGen.print();

//        输出到文件系统
        FileSink<String> fileSink = FileSink
//                输出行式存储的文件，指定路径，指定编码
                .<String>forRowFormat(new Path("D:/temp"), new SimpleStringEncoder<>("UTF-8"))
//                输出文件的一些配置：文件名的前缀后缀
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("bitzer")
                        .withPartSuffix(".log")
                        .build())
//                文件分桶
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-mm-dd"))
//                文件滚动策略
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(10))
                                .withMaxPartSize(new MemorySize(1024 * 1024))
                                .build()
                )
                .build();

        dataGen.sinkTo(fileSink );

        env.execute();
    }
}

