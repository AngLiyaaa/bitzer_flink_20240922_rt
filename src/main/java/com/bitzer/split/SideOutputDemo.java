package com.bitzer.split;

import com.bitzer.bean.WaterSensor;
import com.bitzer.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("10.15.8.145", 7777)
                .map(new WaterSensorMapFunction());
        /*
        * 总结步骤：
        * 1. 使用process算子
        * 2. 定义OutputTag对象
        * 3. 调用ctx.output
        * 4. 通过主流获取 侧流
        *
        * */

//        分流：watersensor的数据，s1，s2的数据分别分开
//        使用侧输出流来实现
        /*
         * 创建OutputTag对象
         * 第一个参数：标签名
         * 第二个参数：放入侧输出流中的 数据的 类型，Typeinformation
         *
         * */
        OutputTag<WaterSensor> s1Tag = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2Tag = new OutputTag<>("s2", Types.POJO(WaterSensor.class));

        SingleOutputStreamOperator<WaterSensor> process = sensorDS.process(
                new ProcessFunction<WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                        String id = value.getId();
                        if ("s1".equals(id)) {
//                            如果是s1，放到侧输出流s1中
                            /*
                             * 上下文调用output，将数据放入侧输出流
                             * 第一个参数：tag对象
                             * 第二个参数：放入测输出流的数据
                             *
                             * */
                            ctx.output(s1Tag, value);
                        } else if ("s2".equals(id)) {
//                        如果是s2，放到侧输出流s2中
                            ctx.output(s2Tag, value);
                        } else {
//                        非s1、s2的数据，放到主流中
                            out.collect(value);
                        }
                    }
                }
        );

//        打印主流
        process.print("主流-非s1s2");

//        从主流中根据标签 获取 打印侧输出流s1
        SideOutputDataStream<WaterSensor> s1 = process.getSideOutput(s1Tag);
        SideOutputDataStream<WaterSensor> s2 = process.getSideOutput(s2Tag);

//        打印侧输出流s2
        s1.print("s1");
        s2.print("s2");

        env.execute();
    }
}
