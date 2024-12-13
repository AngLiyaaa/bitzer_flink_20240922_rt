package com.atguigu.transform;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.FilterFunctionImpl;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1l, 1),
                new WaterSensor("s1", 11l, 11),
                new WaterSensor("s2", 2l, 2),
                new WaterSensor("s3", 3l, 3)
        );

        //filter：true保留，false过滤掉
//        SingleOutputStreamOperator<WaterSensor> filter = sensorDS.filter(new FilterFunction<WaterSensor>() {
//            @Override
//            public boolean filter(WaterSensor value) throws Exception {
//                return "s1".equals(value.getId());
//            }
//        });
        SingleOutputStreamOperator<WaterSensor> filter = sensorDS.filter(new FilterFunctionImpl("s1"));

        filter.print();

        env.execute();
    }

}
