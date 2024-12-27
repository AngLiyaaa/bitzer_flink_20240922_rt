package com.bitzer_test.prd_line;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.sqlserver.SqlServerSource;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class get_sap_api_cdc_test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DebeziumSourceFunction<String> MSsqlSource = SqlServerSource.<String>builder()
                .hostname("10.15.9.41")
                .port(1433)
                .database("Ods_Kpi")
                .tableList("Ods_Kpi.dbo.test")
                .username("s1000")
                .password("Start12321!")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.latest())
                .build();

        env.addSource(MSsqlSource)
                .print();

        env.execute();
    }
}
