package com.bitzer.sink;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class SinkKafkaWithKeyDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        必须开启checkpoint，否则在 精准一次 无法写入kafka
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        SingleOutputStreamOperator<String> sensorDS = env
                .socketTextStream("10.15.8.145", 7777);

//        kafka sink
        /*
        * 如果要指定写入kafka的key
        * 可以自定义反序列器：
        * 1、 实现一个接口，重写 序列化方法
        * 2、 指定key，转成 字节数组
        * 3、 指定value,  转成 字节数组
        * 4、 返回一个 producerrecord对象，把key、value放进去
        *
        * */
        KafkaSink<String> kafkaSink = KafkaSink
                .<String>builder()
                .setBootstrapServers("10.15.8.145:9092")
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<String>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(String element, KafkaSinkContext context, Long timestamp) {
                                String[] datas = element.split(",");
                                byte[] key = datas[0].getBytes(StandardCharsets.UTF_8);
                                byte[] value = element.getBytes(StandardCharsets.UTF_8);
                                return new ProducerRecord<>("paperless_csv_test_20241120", key, value);
                            }
                        }
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("bitzer-")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60*10*1000+"")
                .build();

        sensorDS.sinkTo(kafkaSink);

        env.execute();
    }
}
