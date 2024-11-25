package com.atguigu.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatmapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1l, 1),
                new WaterSensor("s1", 11l, 11),
                new WaterSensor("s2", 2l, 2),
                new WaterSensor("s3", 3l, 3)
        );

        //flatmap：一进多出
        SingleOutputStreamOperator<String> flatMap = sensorDS.flatMap(new FlatMapFunction<WaterSensor, String>() {
            @Override
            public void flatMap(WaterSensor value, Collector<String> out) throws Exception {
                if ("s1".equals(value.getId())) {
                    //如果是s1，输出vc
                    out.collect(value.getVc().toString());
                } else if ("s2".equals(value.getId())) {
                    //如果是s2，分别输出ts和vc
                    out.collect(value.getTs().toString());
                    out.collect(value.getVc().toString());
                }
            }
        });

        flatMap.print();

        env.execute();
    }

}
