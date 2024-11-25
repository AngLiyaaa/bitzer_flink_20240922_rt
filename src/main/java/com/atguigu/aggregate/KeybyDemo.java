package com.atguigu.aggregate;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeybyDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1l, 1),
                new WaterSensor("s1", 1111l, 1111),
                new WaterSensor("s2", 2l, 2),
                new WaterSensor("s3", 3l, 3)
        );

        //按照key(id)来分区，不是分区，是分组
        /*
        * 要点：
        * 1.返回的是一个KeyedStream，键控流
        * 2.keybu不是 转换算子，只是对数据进行重分区，不能设置并行度
        * 3.keyby 分组 与 分区 的 关系
        *       1 keybu是对数据分组，保证相同key的数据在同一个分区
        *       2 分区：一个子任务，可以理解为一个分区
        *
        * */
        KeyedStream<WaterSensor, String> senorKS = sensorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        senorKS.print();

        env.execute();
    }

}
