package com.bitzer_test.csv_for_paperless;

import com.bitzer_test.udf.MergeStepsFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class gaodianya_sql {
    public static void main(String[] args) throws Exception {
        // 设置Flink流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 创建TableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 定义Kafka source
        tableEnv.executeSql(
                "CREATE TABLE kafka_source (" +
                        "  Test_GUID STRING," +
                        "  Step STRING," +
                        "  Test_step_definition STRING," +
                        "  Operator STRING," +
                        "  Date STRING," +
                        "  Time STRING," +
                        "  Timestamp STRING," +
                        "  WATERMARK FOR Timestamp AS Timestamp - INTERVAL '5' SECOND" + // 可选：定义水印策略
                        ") WITH (" +
                        "  'connector' = 'kafka'," +
                        "  'topic' = 'your_topic_name'," +
                        "  'properties.bootstrap.servers' = 'localhost:9092'," +
                        "  'properties.group.id' = 'testGroup'," +
                        "  'format' = 'json'," +
                        "  'scan.startup.mode' = 'earliest-offset'" + // 从最早的offset开始读取
                        ")"
        );

        // 创建一个视图，过滤掉标题行
        tableEnv.executeSql(
                "CREATE VIEW filtered_view AS " +
                        "SELECT * FROM kafka_source WHERE Test_GUID != 'Test GUID'"
        );

        // 创建一个视图，过滤掉标题行
        tableEnv.executeSql(
                "CREATE VIEW filtered_view AS " +
                        "SELECT * FROM kafka_source WHERE Test_GUID != 'Test GUID'"
        );

        // 使用自定义的TableFunction来合并两条记录
        tableEnv.createTemporaryFunction("mergeSteps", MergeStepsFunction.class);

        // 创建一个包含合并后数据的表
        tableEnv.executeSql(
                "CREATE TABLE merged_table AS " +
                        "SELECT mergeSteps(Test_GUID, Step, Test_step_definition, Operator, Date, Time, Timestamp) " +
                        "FROM filtered_view " +
                        "GROUP BY Test_GUID"
        );

        // 定义SQL Server sink
        tableEnv.executeSql(
                "CREATE TABLE sql_server_sink (" +
                        "  Test_GUID STRING," +
                        "  Step STRING," +
                        "  Test_step_definition STRING," +
                        "  Operator STRING," +
                        "  Date STRING," +
                        "  Time STRING," +
                        "  Timestamp STRING," +
                        "  Test_GUID2 STRING," +
                        "  Step2 STRING," +
                        "  Test_step_definition2 STRING," +
                        "  Operator2 STRING," +
                        "  Date2 STRING," +
                        "  Time2 STRING," +
                        "  Timestamp2 STRING" +
                        ") WITH (" +
                        "  'connector' = 'jdbc'," +
                        "  'url' = 'jdbc:sqlserver://your_sql_server_address;databaseName=your_database;encrypt=true;trustServerCertificate=true;loginTimeout=30;'," +
                        "  'table-name' = 'your_table_name'," +
                        "  'username' = 'your_username'," +
                        "  'password' = 'your_password'," +
                        "  'driver' = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'" +
                        ")"
        );

        // 将合并后的数据插入到SQL Server sink中
        tableEnv.executeSql(
                "INSERT INTO sql_server_sink SELECT * FROM merged_table"
        );

        env.execute("Kafka to Table Example");

    }
}