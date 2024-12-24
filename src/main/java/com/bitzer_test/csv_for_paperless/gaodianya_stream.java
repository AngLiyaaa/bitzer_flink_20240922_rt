package com.bitzer_test.csv_for_paperless;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

import java.util.Objects;

public class gaodianya_stream {

    public static void main(String[] args) throws Exception {
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 假设 input 是你的 DataStream<String>, 每个元素是一个 JSON 字符串
        DataStream<String> input = env.fromElements(
                "[{\"id\": 1, \"name\": \"Alice\", \"age\": 25}, {\"id\": 2, \"name\": \"Bob\", \"age\": 30}]",
                "[{\"id\": 3, \"name\": \"Charlie\", \"age\": 35}, {\"id\": 4, \"name\": \"David\", \"age\": 40}]"
        ); // 你的数据源

        // 将输入流中的JSON字符串解析为JSONObject，并过滤掉标题行
        DataStream<JSONObject> jsonStream = input
                .map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        JSONObject json = new JSONObject(value);
                        if (!"Test GUID".equals(json.getString("Test_GUID"))) {
                            return json;
                        }
                        return null; // 返回null以过滤掉标题行
                    }
                })
                .filter(Objects::nonNull); // 过滤掉null值

        // 按 Test_GUID 分组，并使用 RichFlatMapFunction 来合并步骤
        DataStream<JSONObject> transformedStream = jsonStream
                .keyBy(json -> json.getString("Test_GUID")) // 按 Test_GUID 分组
                .flatMap(new MergeStepsFlatMap());

        // 打印结果或写入其他目标
        transformedStream.print();

        // 执行程序
        env.execute("Transform Stream Data");
    }

    // 自定义 FlatMap 函数来合并具有相同 Test_GUID 的两个步骤
    private static class MergeStepsFlatMap extends RichFlatMapFunction<JSONObject, JSONObject> {
        private JSONObject step1 = null;

        @Override
        public void flatMap(JSONObject value, Collector<JSONObject> out) throws Exception {
            if (step1 == null) {
                // 第一次遇到的步骤保存为 step1
                step1 = value;
            } else {
                // 已经有了 step1，现在遇到了 step2，可以进行合并
                JSONObject merged = new JSONObject();
                merged.put("Test_GUID", step1.getString("Test_GUID"));
                merged.put("Step", step1.getString("Step"));
                merged.put("Test_step_definition", step1.getString("Test_step_definition"));
                merged.put("Operator", step1.getString("Operator"));
                merged.put("Date", step1.getString("Date"));
                merged.put("Time", step1.getString("Time"));
                merged.put("Timestamp", step1.getString("Timestamp"));

                merged.put("Test_GUID2", value.getString("Test_GUID"));
                merged.put("Step2", value.getString("Step"));
                merged.put("Test_step_definition2", value.getString("Test_step_definition"));
                merged.put("Operator2", value.getString("Operator"));
                merged.put("Date2", value.getString("Date"));
                merged.put("Time2", value.getString("Time"));
                merged.put("Timestamp2", value.getString("Timestamp"));

                out.collect(merged);

                // 重置 step1 以便处理下一个 Test_GUID 的步骤
                step1 = null;
            }
        }
    }
}
