package com.atguigu.watermark;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class WatermarkAllowLatenessDemo {
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
                    return element.getTs() * 1000L;
                });

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("10.15.8.145", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(watermarkStrategy);


        OutputTag<WaterSensor> lateTag = new OutputTag<>("late-data", Types.POJO(WaterSensor.class));

        SingleOutputStreamOperator<String> process = sensorDS.keyBy(sensor -> sensor.getId())
//                使用 事件时间语义 的窗口 watermark才能起作用
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
//                推迟2s关窗 迟到的就在这改就行
                .allowedLateness(Time.seconds(2))
//                关窗后的迟到数据放入侧输出流
                .sideOutputLateData(lateTag)
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
                );

        process.print();
//        从主流获取侧输出流，打印
        process.getSideOutput(lateTag).printToErr();

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



/*
 * 乱序与迟到的区别
 * 乱序：数据的顺序乱了，出现 时间小的 比 时间大的 晚来
 * 迟到：数据的时间戳 < 当前的watermark
 *
 * 乱序、迟到的数据的处理
 * 1） watermark中指定，乱序等待时间
 * 2） 如果开窗，设置窗口允许迟到
 *      在关窗之前，迟到数据来了，还能被窗口计算，来一条迟到数据触发一次计算
 *      关窗后，迟到数据不会被计算
 * 3） 关窗后的迟到数据，放入侧输出流
 *
 *
 * 如果watermark等待3s，窗口允许迟到2s，为什么不直接watermark等待5s 或者窗口允许迟到5s
 *      watermark等待时间不会设置太大： 影响计算的延迟
 *          3s 窗口第一次触发计算和输出， 13s的数据来， 13 -3 =10s
 *          5s 窗口第一次触发计算和输出， 15s的数据来， 15 -5 =10s
 *      窗口允许迟到，是对 大部分迟到数据的处理， 尽量让结果准确
 *          如果只设置 允许迟到5s， 那么 就会导致 频繁 重新输出
 *
 * 设置经验
 * 1、watermark等待时间，设置一个不算特别大的， 一般是秒级，在乱序 和 延迟 取舍
 * 2、设置一定的窗口允许迟到，只考虑大部分的迟到数据，极端小部分迟到很久的数据，不管
 * 3、极端小部分迟到很久的数据，放到侧输出流。 获取到之后可以做各种处理
 *
 * */