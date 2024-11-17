package com.atguigu.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CollectionDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        env.execute();

    }
}
