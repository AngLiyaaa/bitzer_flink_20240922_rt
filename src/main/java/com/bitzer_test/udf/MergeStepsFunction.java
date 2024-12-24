package com.bitzer_test.udf;

import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class MergeStepsFunction extends TableFunction<Row> {

    public void eval(String testGuid, String step, String testStepDefinition, String operator, String date, String time, String timestamp) {
        // 这里我们假设每两个连续的记录是需要合并的
        // 实际应用中可能需要更复杂的逻辑来确保正确匹配
        // 暂时只处理第一个记录，第二个记录会在下一次调用eval时处理
        collect(Row.of(
                testGuid, step, testStepDefinition, operator, date, time, timestamp,
                null, null, null, null, null, null, null
        ));
    }

    // 在实际应用中，你可能需要在Flink作业中维护状态，以便在接收到第二个记录时进行合并
    // 这里只是一个简化的例子
}