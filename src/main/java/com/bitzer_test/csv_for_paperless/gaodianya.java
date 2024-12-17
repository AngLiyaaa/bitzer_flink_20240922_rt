package com.bitzer_test.csv_for_paperless;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class gaodianya {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //从Kafka读：新Source架构

        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setBootstrapServers("10.15.8.145:9092")
                .setGroupId("leon_li")
                .setTopics("paperless_csv_test_20241120")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();
        env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kasource")
                .print();

        env.execute();
    }
}
