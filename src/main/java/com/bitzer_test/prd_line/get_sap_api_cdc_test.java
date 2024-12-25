package com.bitzer_test.prd_line;

import org.apache.flink.cdc.connectors.sqlserver.SqlServerSource;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class get_sap_api_cdc_test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SqlServerSource.Builder<Object> msSqlSource = SqlServerSource.builder()
                .hostname("10.15.9.41")
                .port(1433)
                .database("Ods_Kpi")
                .username("s1000")
                .password("Start12321!")
                .tableList("Ods_Kpi.dbo.test")
//                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();


        env.execute();
    }
}
