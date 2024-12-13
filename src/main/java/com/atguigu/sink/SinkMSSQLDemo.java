package com.atguigu.sink;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.WaterSensorMapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class SinkMSSQLDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("10.15.8.145", 7777)
                .map(new WaterSensorMapFunction());

        /*
        * 写入sql server
        * 1、 只能用老的sink写法：addsink
        * 2、 jdbcsink的4个参数：
        *   一： 执行的sql，一般就是 insert into
        *   二： 预编译sql，对占位符填充值
        *   三： 执行选项， 攒批、重试
        *   四： 连接选项， url、用户名、密码
        *
        *
        *
        * */
        SinkFunction<WaterSensor> jdbcsink = JdbcSink.sink(
                "insert into flink_test_ws_20241203 values(?,?,?)",
                (JdbcStatementBuilder<WaterSensor>) (preparedStatement, waterSensor) -> {
                    preparedStatement.setString(1, waterSensor.getId());
                    preparedStatement.setLong(2, waterSensor.getTs());
                    preparedStatement.setInt(3, waterSensor.getVc());
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(100)
                        .withBatchIntervalMs(3000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:sqlserver://10.15.9.40:1433;databaseName=bigdata;trustServerCertificate=true")
                        .withUsername("test_flink")
                        .withPassword("Flink123!")
                        .withConnectionCheckTimeoutSeconds(60)
                        .build()
        );

        sensorDS.addSink(jdbcsink);

        env.execute();
    }
}
