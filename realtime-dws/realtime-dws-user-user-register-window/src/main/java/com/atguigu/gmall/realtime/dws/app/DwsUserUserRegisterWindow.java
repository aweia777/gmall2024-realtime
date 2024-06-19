package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.UserRegisterBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsUserUserRegisterWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsUserUserRegisterWindow().start(10035, 1,
                "DwsUserUserRegisterWindow",
                Constant.TOPIC_DWD_USER_REGISTER,
                OffsetsInitializer.earliest());
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
//        stream.print();
//        将数据从string转为JSONObject
        SingleOutputStreamOperator<UserRegisterBean> jsonObjStream = stream.flatMap(new FlatMapFunction<String, UserRegisterBean>() {
            @Override
            public void flatMap(String value, Collector<UserRegisterBean> out) throws Exception {

                JSONObject jsonObject = JSONObject.parseObject(value);
                String create_time = jsonObject.getString("create_time");
                String id = jsonObject.getString("id");
                if (create_time != null && id != null){
                    out.collect(new UserRegisterBean("","","",1L,create_time));
                }

            }
        });

//        添加水位线
        SingleOutputStreamOperator<UserRegisterBean> streamWithWaterMark = jsonObjStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(10L))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
                    @Override
                    public long extractTimestamp(UserRegisterBean element, long recordTimestamp) {
                        return DateFormatUtil.dateTimeToTs(element.getCreate_time());
                    }
                }));

//        开窗
        SingleOutputStreamOperator<UserRegisterBean> reduceStream = streamWithWaterMark.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                    @Override
                    public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                        value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                        return value1;
                    }
                }, new ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>.Context context,
                                        Iterable<UserRegisterBean> iterable,
                                        Collector<UserRegisterBean> collector) throws Exception {
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDate(System.currentTimeMillis());
                        for (UserRegisterBean userRegisterBean : iterable) {
                            userRegisterBean.setStt(stt);
                            userRegisterBean.setEdt(edt);
                            userRegisterBean.setCurDate(curDt);
                            collector.collect(userRegisterBean);
                        }

                    }
                });
//        reduceStream.print();
//        写出
        reduceStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_USER_USER_REGISTER_WINDOW));


    }
}
