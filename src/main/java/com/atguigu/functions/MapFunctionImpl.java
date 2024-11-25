package com.atguigu.functions;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class MapFunctionImpl implements MapFunction<WaterSensor,String> {
    @Override
    public String map(WaterSensor value) throws Exception {
        return value.getId();
    }
}
