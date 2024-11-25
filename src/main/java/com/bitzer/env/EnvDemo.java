package com.bitzer.env;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class EnvDemo {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT,"8082");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        env
//                .socketTextStream("10.15.8.142", 7777)
                .readTextFile("input/word.txt")
                .flatMap(
                        (String s, Collector<Tuple2<String, Integer>> out) -> {
                            String[] words = s.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1));
                            }
                        }
                )
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(s -> s.f0)
                .sum(1)
                .print();
        env.execute();
    }
}
