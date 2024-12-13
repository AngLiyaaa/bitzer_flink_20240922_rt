package com.atguigu.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

public class SinkKafkaDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        必须开启checkpoint，否则在 精准一次 无法写入kafka
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        SingleOutputStreamOperator<String> sensorDS = env
                .socketTextStream("10.15.8.145", 7777);

//        kafka sink
        /*
        * 如果要使用 精准一次 写入kafka ， 需要满足以下条件，缺一不可
        * 1. 开启checkpoint
        * 2. 设置 事务前缀
        * 3. 设置事务超时时间： checkpoint间隔 < 事务超时时间 < max的15分钟
        *
        * */
        KafkaSink<String> kafkaSink = KafkaSink
                .<String>builder()
                .setBootstrapServers("10.15.8.145:9092")
//                指定序列化器：指定topic名称 ，具体的序列化
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("paperless_csv_test_20241120")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
//                写到kafka的一致性级别：精准一次，至少一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                如果是精准一次，必须设置 事务的前缀
                .setTransactionalIdPrefix("bitzer-")
//                如果是精准一次，必须设置 事务超时时间: 大于checkpoint间隔，小于max15分钟
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60*10*1000+"")
                .build();

        sensorDS.sinkTo(kafkaSink);

        env.execute();
    }
}
