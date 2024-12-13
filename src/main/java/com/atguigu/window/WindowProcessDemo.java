package com.atguigu.window;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("10.15.8.145", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

//        窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(15)));

//        apply我没写，要过时废弃了
        SingleOutputStreamOperator<String> process = sensorWS.process(
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    /*
                     * s keyby分组的key
                     * context 上下文
                     * elements 存的数据
                     * out 采集器
                     *
                     * */
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
//                        上下文可以拿到window对象，还有其他 侧输出流 等等
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

        env.execute();
    }
}
