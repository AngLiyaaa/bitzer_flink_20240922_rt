package com.atguigu.aggregate;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1l, 1),
                new WaterSensor("s1", 1111l, 1111),
                new WaterSensor("s2", 2l, 2),
                new WaterSensor("s3", 3l, 3)
        );

        KeyedStream<WaterSensor, String> senorKS = sensorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        /*
        * 简单聚合算子
        * 1. keyby之后才能调用
        * 2. 分组内的聚合，对同一个key的数据进行聚合
        * */
        // 传位置索引的，适用于tuple类型，POJO不行
//        SingleOutputStreamOperator<WaterSensor> result = senorKS.sum(2);
//        SingleOutputStreamOperator<WaterSensor> result = senorKS.sum("vc");

        /*
        *  max/maxby的区别
        *       max：只会取比较字段的最大值，非比较字段保留第一次的值
        *       maxby：取比较字段的最大值，同时非比较字段 取 最大值这条数据的值
        * */
//        SingleOutputStreamOperator<WaterSensor> result = senorKS.max("vc");
        SingleOutputStreamOperator<WaterSensor> result = senorKS.maxBy("vc");

        result.print();

        env.execute();
    }

}
