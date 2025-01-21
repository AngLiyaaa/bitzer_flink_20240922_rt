package com.bitzer_test.prd_line;

import com.bitzer_test.udf.ApiUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONObject;

public class get_sap_api_cdc_test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        SqlServerSourceBuilder.SqlServerIncrementalSource<String> MSsqlSource = new SqlServerSourceBuilder<String>()
//                .hostname("10.15.9.41")
//                .port(1433)
//                .databaseList("Ods_Kpi")
//                .tableList("dbo.ods_assy_powerapps_all_stock_2h")
//                .username("s1000")
//                .password("Start12321!")
//                .deserializer(new JsonDebeziumDeserializationSchema())
//                .startupOptions(StartupOptions.latest())
//                .build();
//
//        DataStreamSource<String> source = env.fromSource(MSsqlSource, WatermarkStrategy.noWatermarks(), "MSsqlSource");
        DataStreamSource<String> source = env
                .fromElements("{\"before\":null,\"after\":{\"Serial_Number\":\"1908103089\",\"Shift\":\"早班/Early Shift\"," +
                        "\"Series\":\"HSN7471-75-40P\",\"Family\":\"HS74\",\"Operator\":\"孙常春\",\"Create_Time\":1736941743687," +
                        "\"Remark\":\"\",\"ProductLine\":null,\"ID\":111040},\"source\":{\"version\":\"1.9.7.Final\",\"connector\"" +
                        ":\"sqlserver\",\"name\":\"sqlserver_transaction_log_source\",\"ts_ms\":1736912961510,\"snapshot\":\"false" +
                        "\",\"db\":\"Ods_Kpi\",\"sequence\":null,\"schema\":\"dbo\",\"table\":\"ods_assy_powerapps_all_stock_2h\"," +
                        "\"change_lsn\":\"00000116:00001e3c:0002\",\"commit_lsn\":\"00000116:00001e3c:0004\",\"event_serial_no\":1" +
                        "},\"op\":\"c\",\"ts_ms\":1736912965468,\"transaction\":null}");

        source.print();

        //map或flatmap转换,通过after中的serial_number,生成url，get这个url获得json数据并return json数据
//        DataStream<String> apiDataStream = source
//                .map(new MapFunction<String, String>() {
//                    @Override
//                    public String map(String value) throws Exception {
//                        // 解析JSON，生成URL
//                        String serialNumber = new JSONObject(value).getJSONObject("after").getString("Serial_Number");
//                        // 构建 URL
//                        String url ="https://vhbizpfclb.rot.hec.bitzer.biz/sap/opu/odata/sap/ZCDS_MES_VIEW1_CDS/ZCDS_MES_VIEW1(p_sernr='00000000"
//                                + serialNumber + "')/Set?$top=50&$format=json";
//                        // 发起HTTP GET请求
//                        String username = "PP_ODATA_CN";
//                        String password = "b0ZHT?f=6863lATjkHL3";
//                        String data = ApiUtil.getApiData(url, username, password);
//                        return data;
//                    }
//                });
        DataStream<String> apiDataStream = source
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        // 解析JSON，生成URL
                        JSONObject jsonObject = new JSONObject(value).getJSONObject("after");
                        String serialNumber = jsonObject.getString("Serial_Number");
                        // 构建 URL
                        String url = "https://vhbizpfclb.rot.hec.bitzer.biz/sap/opu/odata/sap/ZCDS_MES_VIEW1_CDS/ZCDS_MES_VIEW1(p_sernr='00000000"
                                + serialNumber + "')/Set?$top=50&$format=json";
                        // 发起HTTP GET请求
                        String username = "PP_ODATA_CN";
                        String password = "b0ZHT?f=6863lATjkHL3";
                        String data = ApiUtil.getApiData(url, username, password);

                        // 解析API结果，提取d.results数组
                        JSONObject apiResponse = new JSONObject(data);
                        JSONArray results = apiResponse.getJSONObject("d").getJSONArray("results");

                        // 遍历数组中的每个元素，并将其输出
                        for (int i = 0; i < results.length(); i++) {
                            out.collect(results.getJSONObject(i).toString());
                        }
                    }
                })
        //增加去重说需要keyby还是不行，暂时放弃，后续要弄明白这里的逻辑才能继续
//                .keyBy(value -> new JSONObject(value).getString("Serial_Number")) // 使用 Serial_Number 作为 key
//                .distinct(); // 增加去重操作
                ;

        apiDataStream.print();



//        apiDataStream.addSink(JdbcSink.sink(
//                "INSERT INTO your_table (column1, column2) VALUES (?, ?)",
//                (ps, t) -> {
//                    ps.setString(1, t[0]);
//                    ps.setString(2, t[1]);
//                },
//                JdbcExecutionOptions.builder()
//                        .withBatchSize(100)
//                        .withBatchIntervalMs(200)
//                        .withMaxRetries(5)
//                        .build(),
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                        .withUrl("jdbc:sqlserver://your_sql_server_host:1433;databaseName=your_database")
//                        .withDriverName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
//                        .withUsername("your_username")
//                        .withPassword("your_password")
//                        .build()
//        ));

        env.execute();
    }
}
