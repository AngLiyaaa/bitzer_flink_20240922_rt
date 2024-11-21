//package com.atguigu.bztest;
//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//public class test20240930 {
//    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//        String sql = "CREATE TABLE csv_table (\n" +
//                "  id STRING,\n" +
//                "  earnings DOUBLE,\n" +
//                "  gender STRING," +
//                "  age INT," +
//                "  region string," +
//                "  education INT  \n" +
//                ") WITH (\n" +
//                "  'connector'='filesystem',\n" +
//                "  'path'='input/CPSSW8.csv',\n" +
//                "  'format'='csv'\n" +
//                ")";
//        tableEnv.executeSql(sql);
//        Table sqlQuery = tableEnv.sqlQuery("select id, earnings from csv_table where earnings > 25");
//        tableEnv.toDataStream(sqlQuery).print();
//        env.execute();
//    }
//}
