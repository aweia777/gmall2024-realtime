package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTrafficHomeDetailPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficHomeDetailPageViewWindow().start(10033  , 4,
                "DwsTrafficHomeDetailPageViewWindow",
                Constant.TOPIC_DWD_TRAFFIC_PAGE, OffsetsInitializer.earliest());
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
//        核心逻辑
//        读取dwd的topic——dwd_traffic_page,筛选出page_id为home或good_detail的
        SingleOutputStreamOperator<JSONObject> jsonObj= stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                JSONObject page = jsonObject.getJSONObject("page");
                String page_id = page.getString("page_id");
                if ("home".equals(page_id) || "good_detail".equals(page_id)) {
                    collector.collect(jsonObject);
                }
            }
        });

//        按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObj.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

//        4.判断独立访客
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream = keyedStream
                .process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {

                    ValueState<String> home_sdt;
                    ValueState<String> good_detail_sdt;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        ValueStateDescriptor<String> home_sdt_desc = new ValueStateDescriptor<>("home_sdt", String.class);
                        home_sdt_desc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());

                        ValueStateDescriptor<String> good_detail_desc = new ValueStateDescriptor<>("good_detail", String.class);
                        good_detail_desc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        home_sdt = getRuntimeContext().getState(home_sdt_desc);
                        good_detail_sdt = getRuntimeContext().getState(good_detail_desc);

                    }

                    @Override
                    public void processElement(JSONObject object,
                                               KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context context,
                                               Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                        JSONObject common = object.getJSONObject("common");
                        String page_id = common.getString("page_id");

                        long homeUvCt = 0L;
                        long detailUvCt = 0L;
                        Long ts = object.getLong("ts");
                        String today = DateFormatUtil.tsToDate(ts);
                        if ("home".equals(page_id)) {
                            String date_in_sdt = home_sdt.value();
                            if (date_in_sdt == null || !date_in_sdt.equals(today)) {
                                homeUvCt = 1L;
                                home_sdt.update(today);
                            }
                        } else {
                            String detail_last_login_dt = good_detail_sdt.value();
                            if (detail_last_login_dt == null || !detail_last_login_dt.equals(today)) {
                                detailUvCt = 1L;
                                good_detail_sdt.update(today);
                            }

                        }

                        if (homeUvCt != 0 || detailUvCt != 0) {
                            collector.collect(TrafficHomeDetailPageViewBean.builder()
                                    .homeUvCt(homeUvCt)
                                    .goodDetailUvCt(detailUvCt)
                                    .curDate(today)
                                            .date_in_sdt(home_sdt.value())
                                            .detail_last_login_dt(good_detail_sdt.value())
                                    .ts(ts)
                                    .build());
                        }
                    }
                });
//        beanStream.print();


//        3.添加水位线，使用beanStream的ts作为事件时间
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> streamWithWaterMark = beanStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficHomeDetailPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                .withTimestampAssigner(new SerializableTimestampAssigner<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public long extractTimestamp(TrafficHomeDetailPageViewBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

//        4.分组开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceStream = streamWithWaterMark.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1,
                                                                TrafficHomeDetailPageViewBean value2) throws Exception {
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        return value1;

                    }
                }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow,
                                      Iterable<TrafficHomeDetailPageViewBean> iterable,
                                      Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                        String stt = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String edt = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (TrafficHomeDetailPageViewBean trafficHomeDetailPageViewBean : iterable) {
                            trafficHomeDetailPageViewBean.setStt(stt);
                            trafficHomeDetailPageViewBean.setEdt(edt);
                            trafficHomeDetailPageViewBean.setCurDate(curDt);
                            collector.collect(trafficHomeDetailPageViewBean);
                        }
                    }
                });


//        5.写出
        reduceStream.map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW));


    }
}
