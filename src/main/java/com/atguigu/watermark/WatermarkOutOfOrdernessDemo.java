package com.atguigu.watermark;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WatermarkOutOfOrdernessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        指定watermark策略
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
//                乱序的watermark，有等待时间 等待3秒
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
//                指定 时间戳分配器，从数据中提取
                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> {
//                        返回的时间戳， 要 毫秒
                    System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                    return element.getTs() * 1000L;
                });

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("10.15.8.145", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(watermarkStrategy);

        sensorDS.keyBy(sensor -> sensor.getId())
//                使用 事件时间语义 的窗口 watermark才能起作用
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .process(
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        long startts = context.window().getStart();
                        long endts = context.window().getEnd();
                        String windowstart = DateFormatUtils.format(startts, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowend = DateFormatUtils.format(endts, "yyyy-MM-dd HH:mm:ss.SSS");
                        long count = elements.spliterator().estimateSize();
                        out.collect("key=" + s + "窗口" + windowstart + "," + windowend + "有" + count + "条数据" + elements.toString());
                    }
                }
        ).print();

        env.execute();
    }
}


/*
* 内置watermark的生成原理
* 1、 都是周期性生成的： 默认200ms
* 2、有序流： watermark =当前最大的时间时间 - 1ms
* 3、乱序流： watermark =当前最大的时间时间 - 延迟时间 - 1ms
*
* */