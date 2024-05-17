package com.atguigu.gmall.realtime.dwd.db.spilt.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseLog().start(10011, 4, "DwdBaseLog", Constant.TOPIC_LOG, OffsetsInitializer.earliest());
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
//        核心业务处理
//        1.etl对数据进行过滤
//        stream.print();
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

//        2.进行新老客户修复
        KeyedStream<JSONObject, String> keyedStream = keyByWithWaterMark(jsonObjStream);

        SingleOutputStreamOperator<JSONObject> isNewFixStream = isNewFix(keyedStream);

        isNewFixStream.print();


    }

    private SingleOutputStreamOperator<JSONObject> isNewFix(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(new KeyedProcessFunction<String,
                JSONObject, JSONObject>() {
            //            创建状态，在open中，保证多节点只运行一次
            ValueState<String> firstLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                firstLoginDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("first_login_dt", String.class));

            }

            @Override
            public void processElement(JSONObject object,
                                       KeyedProcessFunction<String, JSONObject, JSONObject>.Context context,
                                       Collector<JSONObject> collector) throws Exception {
//                1.获取当前数据的is_new字段
                JSONObject common = object.getJSONObject("common");
                String isNew = common.getString("is_new");
                String firstLoginDt = firstLoginDtState.value();
                Long ts = object.getLong("ts");
                String cutDt = DateFormatUtil.tsToDate(ts);
                if ("1".equals(isNew)) {
//                    判断当前状态情况:如果状态不为空，且第一次登录时间又不是同一天，说明不是真的新访客
                    if (firstLoginDt != null && !firstLoginDt.equals(cutDt)) {
                        common.put("is_new", 0);
                    } else if (firstLoginDt == null) {
                        firstLoginDtState.update(cutDt);
                    } else {
//                        留空：当前数据是新访客，登录时间是同一天
                    }
                } else if ("0".equals(isNew)) {
//                    is_new == 0
                    if (firstLoginDt == null) {
//                        超级老用户，flink的状态中没有该用户的信息。把访客的日期设置为curDt的前一天
                        firstLoginDtState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 1000L));
                    } else {
//                        留空：正常情况，无需处理
                    }
                } else {

                }
                collector.collect(object);
            }
        });
    }

    public KeyedStream<JSONObject, String> keyByWithWaterMark(SingleOutputStreamOperator<JSONObject> jsonObjStream) {
        return jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject object, long l) {
//                        这是的时间属性ts来自于数据流中的json的ts，flink中统一使用毫秒，如果数据是秒单位，则需要将ts x 1000
                        return object.getLong("ts");
                    }
//                    安装key聚合，这里的key选择mid
                })).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject object) throws Exception {
                return object.getJSONObject("common").getString("mid");
            }
        });
    }

    /**
     * 过滤筛选数据：日志数据里面的page和start必定有其中之一，只要这两个数据有一个不为空，都视为有效的日志数据
     * 为了保证数据流中的common中的mid，以及ts字段不为空，同样需要判断
     * @param stream kafka的topic_log的数据流
     * @return
     */
    public SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    JSONObject page = jsonObject.getJSONObject("page");
                    JSONObject start = jsonObject.getJSONObject("start");
                    JSONObject common = jsonObject.getJSONObject("common");
                    Long ts = jsonObject.getLong("ts");
                    if (page != null || start != null) {
                        if (common != null && common.getString("mid") != null && ts != null){
                            collector.collect(jsonObject);
                        }
                    }
                } catch (Exception e) {
//                    e.printStackTrace();
                    System.out.println("过滤掉脏数据：" + s);
                }
            }
        });
    }
}
