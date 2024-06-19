package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.UserLoginBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsUserUserLoginWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsUserUserLoginWindow().start(10034, 4, "DwsUserUserLoginWindow",
                Constant.TOPIC_DWD_TRAFFIC_PAGE, OffsetsInitializer.earliest());
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
//        过滤并转换为JSONObject
        SingleOutputStreamOperator<JSONObject> flatMapStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                String uid = common.getString("uid");
                Long ts = jsonObject.getLong("ts");
                String last_page_id = jsonObject.getJSONObject("page").getString("last_page_id");
                if (ts != null && uid != null && (last_page_id == null || "login".equals(last_page_id))) {
                    out.collect(jsonObject);
                }
            }
        });
//        flatMapStream.print();
//        设置水位线
        SingleOutputStreamOperator<JSONObject> streamWithWaterMark = flatMapStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10L))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));
//        按照uid进行分组
        KeyedStream<JSONObject, String> keyedStream = streamWithWaterMark.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("uid");
            }
        });

//        判断独立用户和回流用户
        SingleOutputStreamOperator<UserLoginBean> uuctStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {

            ValueState<String> login_last_dt_state;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> loginLastDtDesc = new ValueStateDescriptor<>("login_last_dt", String.class);
//                这里设置状态的状态存活时间为永久，需要避免新老用户识别问题
                login_last_dt_state = getRuntimeContext().getState(loginLastDtDesc);

            }

            @Override
            public void processElement(JSONObject object,
                                       KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context context,
                                       Collector<UserLoginBean> collector) throws Exception {
//                用当前的日期对比状态中的日期
                String last_login_date = login_last_dt_state.value();
                Long ts = object.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                Long backCt = 0L;
                // 独立用户数
                Long uuCt = 0L;

                if (last_login_date == null || !last_login_date.equals(curDt)) {
                    uuCt = 1L;
                }

                if (last_login_date != null &&  ts - DateFormatUtil.dateToTs(last_login_date) > 7 * 24 * 60 * 60 * 1000L) {
                    backCt = 1L;
                }

                login_last_dt_state.update(curDt);
                if (uuCt != 0){
                    collector.collect(new UserLoginBean("", "", "", backCt, uuCt, ts));
                }

            }
        });
//        uuctStream.print();
//        开窗聚合
        SingleOutputStreamOperator<UserLoginBean> reduceBeanStream = uuctStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        return value1;

                    }
                }, new ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<UserLoginBean,
                            UserLoginBean, TimeWindow>.Context context, Iterable<UserLoginBean> iterable,
                                        Collector<UserLoginBean> collector) throws Exception {
                        String stt = DateFormatUtil.tsToDateTime(context.window().getStart());
                        String edt = DateFormatUtil.tsToDateTime(context.window().getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (UserLoginBean userLoginBean : iterable) {
                            userLoginBean.setEdt(edt);
                            userLoginBean.setStt(stt);
                            userLoginBean.setCurDate(curDt);
                            collector.collect(userLoginBean);
                        }
                    }
                });
//        reduceBeanStream.print();

        reduceBeanStream.map(new DorisMapFunction<>()
                ).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_USER_USER_LOGIN_WINDOW));


    }
}
