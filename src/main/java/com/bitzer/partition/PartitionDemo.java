package com.bitzer.partition;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionDemo {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(2);

        DataStreamSource<String> socketDS = env.socketTextStream("10.15.8.145", 7777);

//        shuffle随机分区：random.nextInt(numberOfChannels)   （下游算子并行度）
//        socketDS.shuffle().print();

//        rebalance轮询：nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels（下游算子并行度）
//        如果是数据倾斜的场景，source读进来之后，调用rebalance，就可以解决读取数据源的数据倾斜
//        socketDS.rebalance().print();

//        rescale缩放：实现轮询，局部组队，比rebalance更高效
//        socketDS.rescale().print();

//        broadcast广播：发送给下游的所有子任务
        socketDS.broadcast().print();

//        global全局：全部发往第一个子任务 return 0
//        socketDS.global().print();

//        keyby：按指定key去发送，相同key发往同一个子任务

        env.execute();
    }
}
