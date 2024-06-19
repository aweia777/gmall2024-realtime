package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.gmall.realtime.common.bean.TrafficPageViewBean;
import org.apache.flink.api.common.functions.MapFunction;

public class DorisMapFunction<T> implements MapFunction<T,String> {
    @Override
    public String map(T t) throws Exception {
                SerializeConfig config = new SerializeConfig();
                config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
                return JSONObject.toJSONString(t,config);

        }
    }

