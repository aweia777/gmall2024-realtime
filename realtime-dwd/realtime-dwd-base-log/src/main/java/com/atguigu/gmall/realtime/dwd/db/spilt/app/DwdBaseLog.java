package com.atguigu.gmall.realtime.dwd.db.spilt.app;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

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

//        3.拆分不同类型的用户行为日志
//        启动日志：启动信息     报错信息
//        页面日志：页面信息     曝光信息    动作信息    报错信息
//        使用侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start", TypeInformation.of(String.class));
        OutputTag<String> errorTag = new OutputTag<String>("err", TypeInformation.of(String.class));
        OutputTag<String> displayTag = new OutputTag<String>("display", TypeInformation.of(String.class));
        OutputTag<String> actionTag = new OutputTag<String>("action", TypeInformation.of(String.class));

        SingleOutputStreamOperator<String> pageStream = splitLog(isNewFixStream, startTag, errorTag, displayTag, actionTag);
        SideOutputDataStream<String> startStream = pageStream.getSideOutput(startTag);
        SideOutputDataStream<String> errStream = pageStream.getSideOutput(errorTag);
        SideOutputDataStream<String> displayStream = pageStream.getSideOutput(displayTag);
        SideOutputDataStream<String> actionStream = pageStream.getSideOutput(actionTag);

//        pageStream.print("page");
//        startStream.print("start");
//        errStream.print("err");
//        displayStream.print("display");
//        actionStream.print("action");


        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));

//

    }




    public SingleOutputStreamOperator<String> splitLog(SingleOutputStreamOperator<JSONObject> isNewFixStream, OutputTag<String> startTag, OutputTag<String> errorTag, OutputTag<String> displayTag, OutputTag<String> actionTag) {
        return isNewFixStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject object,
                                       ProcessFunction<JSONObject, String>.Context context,
                                       Collector<String> collector) throws Exception {
//                根据不同的数据，拆分到不同的侧输出流
                JSONObject err = object.getJSONObject("err");
                if (err != null) {
                    context.output(errorTag, err.toJSONString());
                    object.remove("err");
                }
                JSONObject page = object.getJSONObject("page");
                JSONObject start = object.getJSONObject("start");
                JSONObject common = object.getJSONObject("common");
                Long ts = object.getLong("ts");
                if (start != null) {
//                    当前为启动日志
                    context.output(startTag, object.toJSONString());
                } else if (page != null) {
//                    当前为页面日志
                    JSONArray displays = object.getJSONArray("displays");
                    if (displays != null){
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("ts", ts);
                            display.put("page", page);
                            context.output(displayTag, display.toJSONString());
                        }
                        object.remove("displays");
                    }


                    JSONArray actions = object.getJSONArray("actions");
                    if ( actions != null){
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("ts", ts);
                            action.put("page", page);
                            context.output(actionTag, action.toJSONString());
                        }
                        object.remove("actions");
                    }


                    collector.collect(object.toJSONString());
                }
            }
        });
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
