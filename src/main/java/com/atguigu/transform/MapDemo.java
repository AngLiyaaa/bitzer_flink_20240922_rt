package com.atguigu.transform;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.MapFunctionImpl;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1l, 1),
                new WaterSensor("s2", 2l, 2),
                new WaterSensor("s3", 3l, 3)
        );

        //map算子：一进一出

        //方式1：匿名实现类
//        SingleOutputStreamOperator<String> map = sensorDS.map(new MapFunction<WaterSensor, String>() {
//            @Override
//            public String map(WaterSensor waterSensor) throws Exception {
//                return waterSensor.getId();
//            }
//        });

        //方式2：lambda表达式
//        SingleOutputStreamOperator<String> map = sensorDS.map(sensor -> sensor.getId());

        //方式3：定义一个类来实现mapfunction
//        SingleOutputStreamOperator<String> map = sensorDS.map(new MyMapFunction());
        SingleOutputStreamOperator<String> map = sensorDS.map(new MapFunctionImpl());

        map.print();

        env.execute();
    }

//    public static class MyMapFunction implements MapFunction<WaterSensor, String> {
//
//        @Override
//        public String map(WaterSensor waterSensor) throws Exception {
//            return waterSensor.getId();
//        }
//    }
}
