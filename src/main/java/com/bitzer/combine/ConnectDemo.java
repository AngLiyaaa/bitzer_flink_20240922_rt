package com.bitzer.combine;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3);
//        DataStreamSource<String> source2 = env.fromElements("a", "b", "v");
        SingleOutputStreamOperator<Integer> source1 = env.socketTextStream("10.15.8.145", 7777).map(i -> Integer.parseInt(i));
        DataStreamSource<String> source2 = env.socketTextStream("10.15.8.145", 8888);

        /*
        * 使用connect合流
        * 1. 一次只能连接2条流
        * 2. 流的数据类型可以不一样
        * 3. 连接后可以调用map、flatmap、process来处理，但是各处理各的
        * */
        ConnectedStreams<Integer, String> connect = source1.connect(source2);

        SingleOutputStreamOperator<String> result = connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return value.toString();
            }

            @Override
            public String map2(String value) throws Exception {
                return value;
            }
        });

        result.print();

        env.execute();
    }
}
