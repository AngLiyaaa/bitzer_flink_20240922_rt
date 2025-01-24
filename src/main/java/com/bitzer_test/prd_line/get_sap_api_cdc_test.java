package com.bitzer_test.prd_line;

import com.bitzer_test.bean.sap_api_kpi;
import com.bitzer_test.udf.ApiUtil;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Date;

public class get_sap_api_cdc_test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SqlServerSourceBuilder.SqlServerIncrementalSource<String> MSsqlSource = new SqlServerSourceBuilder<String>()
                .hostname("10.15.9.41")
                .port(1433)
                .databaseList("Ods_Kpi")
                .tableList("dbo.ods_assy_powerapps_all_stock_2h")
                .username("s1000")
                .password("Start12321!")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.latest())
                .build();
        DataStreamSource<String> source = env.fromSource(MSsqlSource, WatermarkStrategy.noWatermarks(), "MSsqlSource");


//        DataStreamSource<String> source = env
//                .fromElements("{\"before\":null,\"after\":{\"Serial_Number\":\"1908103089\",\"Shift\":\"早班/Early Shift\"," +
//                        "\"Series\":\"HSN7471-75-40P\",\"Family\":\"HS74\",\"Operator\":\"孙常春\",\"Create_Time\":1736941743687," +
//                        "\"Remark\":\"\",\"ProductLine\":null,\"ID\":111040},\"source\":{\"version\":\"1.9.7.Final\",\"connector\"" +
//                        ":\"sqlserver\",\"name\":\"sqlserver_transaction_log_source\",\"ts_ms\":1736912961510,\"snapshot\":\"false" +
//                        "\",\"db\":\"Ods_Kpi\",\"sequence\":null,\"schema\":\"dbo\",\"table\":\"ods_assy_powerapps_all_stock_2h\"," +
//                        "\"change_lsn\":\"00000116:00001e3c:0002\",\"commit_lsn\":\"00000116:00001e3c:0004\",\"event_serial_no\":1" +
//                        "},\"op\":\"c\",\"ts_ms\":1736912965468,\"transaction\":null}");

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
        DataStream<sap_api_kpi> apiDataStream = source
                .flatMap(new FlatMapFunction<String, sap_api_kpi>() {
                    @Override
                    public void flatMap(String value, Collector<sap_api_kpi> out) throws Exception {
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
//                            out.collect(results.getJSONObject(i).toString());
                            JSONObject result = results.getJSONObject(i);
                            sap_api_kpi kpi = new sap_api_kpi();
                            kpi.SerialNumber = result.getString("SerialNumber");
                            kpi.UserName = result.getString("UserName");
//                            kpi.PostingDate = result.getString("PostingDate");
//                            kpi.PostingDate = new Date(Long.parseLong(result.getString("PostingDate").substring(6, result.getString("PostingDate").length() - 2)));
                            kpi.PostingDate = result.getString("PostingDate") != null ? new Date(Long.parseLong(result.getString("PostingDate").substring(6, result.getString("PostingDate").length() - 2))) : null;
                            kpi.ObjectId = result.getString("ObjectId");
//                            kpi.StartExecutionDate = result.getString("StartExecutionDate");
//                            kpi.StartExecutionDate = new Date(Long.parseLong(result.getString("StartExecutionDate").substring(6, result.getString("StartExecutionDate").length() - 2)));
                            kpi.StartExecutionDate = result.getString("StartExecutionDate") != null ? new Date(Long.parseLong(result.getString("StartExecutionDate").substring(6, result.getString("StartExecutionDate").length() - 2))) : null;
                            kpi.StartExecutionTime = result.getString("StartExecutionTime");
                            kpi.UniqueAdditFieldID = result.getString("UniqueAdditFieldID");
                            kpi.AdditionalField = result.getString("AdditionalField");
                            kpi.AdditionalFieldData = result.getString("AdditionalFieldData");
//                            kpi.AdditemDate = result.getString("AdditemDate");
//                            kpi.AdditemDate = new Date(Long.parseLong(result.getString("AdditemDate").substring(6, result.getString("AdditemDate").length() - 2)));
                            kpi.AdditemDate = result.getString("AdditemDate") != null ? new Date(Long.parseLong(result.getString("AdditemDate").substring(6, result.getString("AdditemDate").length() - 2))) : null;
                            kpi.AdditemTime = result.getString("AdditemTime");
                            kpi.OrderNumber = result.getString("OrderNumber");
                            kpi.ActivityNumber = result.getString("ActivityNumber");
                            kpi.MaterialNumber = result.getString("MaterialNumber");
//                            kpi.EntryDateOfConfirmation = result.getString("EntryDateOfConfirmation");
//                            kpi.EntryDateOfConfirmation = new Date(Long.parseLong(result.getString("EntryDateOfConfirmation").substring(6, result.getString("EntryDateOfConfirmation").length() - 2)));
                            kpi.EntryDateOfConfirmation = result.getString("EntryDateOfConfirmation") != null ? new Date(Long.parseLong(result.getString("EntryDateOfConfirmation").substring(6, result.getString("EntryDateOfConfirmation").length() - 2))) : null;
                            kpi.ConfirmationEntryTime = result.getString("ConfirmationEntryTime");
                            kpi.User_Name = result.getString("User_Name");
                            kpi.SalesOrderNumber = result.getString("SalesOrderNumber");
                            kpi.SalesOrderItemNumber = result.getString("SalesOrderItemNumber");
                            kpi.OrderType = result.getString("OrderType");
                            kpi.MaterialType = result.getString("MaterialType");
                            kpi.ControlKey = result.getString("ControlKey");
                            kpi.StorageLocation = result.getString("StorageLocation");
                            kpi.Plant = result.getString("Plant");
                            kpi.ProductType = result.getString("ProductType");
                            kpi.Family = result.getString("Family");
                            kpi.Characteristic = result.getString("Characteristic");

                            out.collect(kpi);
                        }
                    }
                })
                //增加去重说需要keyby还是不行，暂时放弃，后续要弄明白这里的逻辑才能继续
//                .keyBy(value -> new JSONObject(value).getString("Serial_Number")) // 使用 Serial_Number 作为 key
//                .distinct(); // 增加去重操作
                ;

        apiDataStream.print();

        apiDataStream.addSink(JdbcSink.sink(
                "insert into [ods_sap_all_json_detail_2h] ([SerialNumber],\n" +
                        "                        [UserName], [PostingDate], [ObjectId], [StartExecutionDate], [StartExecutionTime],\n" +
                        "                        [UniqueAdditFieldID], [AdditionalField], [AdditionalFieldData], [AdditemDate],\n" +
                        "                        [AdditemTime], [OrderNumber], [ActivityNumber], [MaterialNumber], [EntryDateOfConfirmation],\n" +
                        "                        [ConfirmationEntryTime], [User_Name], [SalesOrderNumber], [SalesOrderItemNumber],\n" +
                        "                        [OrderType], [MaterialType], [ControlKey], [StorageLocation], [Plant], [ProductType],\n" +
                        "                        [Family], [Characteristic])values (?, ?, ?, ?, ?, ?, ?, ?,\n" +
                        "                        ?, ?,?, ?, ?, ?, ?,?, ?, ?, ?, ?,?, ?,\n" +
                        "                        ?, ?, ?, ?, ?)",
                (ps, sap_api_kpi) -> {
                    ps.setString(1, sap_api_kpi.SerialNumber);
                    ps.setString(2, sap_api_kpi.UserName);
                    ps.setDate(3, sap_api_kpi.PostingDate);
                    ps.setString(4, sap_api_kpi.ObjectId);
                    ps.setDate(5, sap_api_kpi.StartExecutionDate);
                    ps.setString(6, sap_api_kpi.StartExecutionTime);
                    ps.setString(7, sap_api_kpi.UniqueAdditFieldID);
                    ps.setString(8, sap_api_kpi.AdditionalField);
                    ps.setString(9, sap_api_kpi.AdditionalFieldData);
                    ps.setDate(10, sap_api_kpi.AdditemDate);
                    ps.setString(11, sap_api_kpi.AdditemTime);
                    ps.setString(12, sap_api_kpi.OrderNumber);
                    ps.setString(13, sap_api_kpi.ActivityNumber);
                    ps.setString(14, sap_api_kpi.MaterialNumber);
                    ps.setDate(15, sap_api_kpi.EntryDateOfConfirmation);
                    ps.setString(16, sap_api_kpi.ConfirmationEntryTime);
                    ps.setString(17, sap_api_kpi.User_Name);
                    ps.setString(18, sap_api_kpi.SalesOrderNumber);
                    ps.setString(19, sap_api_kpi.SalesOrderItemNumber);
                    ps.setString(20, sap_api_kpi.OrderType);
                    ps.setString(21, sap_api_kpi.MaterialType);
                    ps.setString(22, sap_api_kpi.ControlKey);
                    ps.setString(23, sap_api_kpi.StorageLocation);
                    ps.setString(24, sap_api_kpi.Plant);
                    ps.setString(25, sap_api_kpi.ProductType);
                    ps.setString(26, sap_api_kpi.Family);
                    ps.setString(27, sap_api_kpi.Characteristic);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(2000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:sqlserver://10.15.9.40:1433;databaseName=bigdata;trustServerCertificate=true")
//                        .withDriverName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
                        .withUsername("s1000")
                        .withPassword("Start12321!")
                        .withConnectionCheckTimeoutSeconds(60)
                        .build()
        ));

        env.execute();
    }
}
