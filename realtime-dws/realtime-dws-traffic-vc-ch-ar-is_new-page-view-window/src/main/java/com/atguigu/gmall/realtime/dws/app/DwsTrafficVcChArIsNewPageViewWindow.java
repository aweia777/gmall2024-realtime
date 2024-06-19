package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.reflections.util.ConfigurationBuilder.build;

public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindow().start(10032,4,
                "DwsTrafficVcChArIsNewPageViewWindow",
                Constant.TOPIC_DWD_TRAFFIC_PAGE, OffsetsInitializer.earliest());
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
//        1.读取dwd的page_topic
//        2.进行清洗过滤，并转换成jsonObj结构
        SingleOutputStreamOperator<JSONObject> jsonObjStream = getJsonObjStream(stream);

//        3.按照mid进行分类
        SingleOutputStreamOperator<TrafficPageViewBean> beanStream = getBeanStream(jsonObjStream);

//        beanStream.print();

//        4.添加水位线
        SingleOutputStreamOperator<TrafficPageViewBean> withWaterMarkStream = getWithWaterMarkStream(beanStream);

//        5.按照粒度进行分组
        KeyedStream<TrafficPageViewBean, String> keyedStream = getKeyedStream(withWaterMarkStream);

//        6.开窗
        WindowedStream<TrafficPageViewBean, String, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));


//        7.聚合
        SingleOutputStreamOperator<TrafficPageViewBean> reduceFullStream = getReduceFullStream(windowStream);

//        reduceFullStream.print();

//        8.写出
        reduceFullStream.map(new DorisMapFunction<TrafficPageViewBean>())
//                .print();
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW));

    }

    private SingleOutputStreamOperator<TrafficPageViewBean> getReduceFullStream(WindowedStream<TrafficPageViewBean, String, TimeWindow> windowStream) {
        return windowStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
//                将多个元素的度量值累加到一起：v1表示累加的结果（第一次调用的时候表示第一个元素） v2表示下一个要加进来的数
                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                return value1;
            }
        }, new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
            @Override
            public void process(String s,
                                ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>.Context context,
                                Iterable<TrafficPageViewBean> iterable,
                                Collector<TrafficPageViewBean> collector) throws Exception {
                TimeWindow window = context.window();
                String stt = DateFormatUtil.tsToDate(window.getStart());
                String edt = DateFormatUtil.tsToDate(window.getEnd());
                String partition = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                for (TrafficPageViewBean trafficPageViewBean : iterable) {
                    trafficPageViewBean.setStt(stt);
                    trafficPageViewBean.setEdt(edt);
                    trafficPageViewBean.setCur_date(partition);
                    collector.collect(trafficPageViewBean);
                }

            }


        });
    }

    private KeyedStream<TrafficPageViewBean, String> getKeyedStream(SingleOutputStreamOperator<TrafficPageViewBean> withWaterMarkStream) {
        return withWaterMarkStream.keyBy(new KeySelector<TrafficPageViewBean, String>() {
            @Override
            public String getKey(TrafficPageViewBean value) throws Exception {
                return value.getVc() + ":" + value.getCh() + ":" + value.getAr() + ":" + value.getIsNew();
            }
        });
    }

    private SingleOutputStreamOperator<TrafficPageViewBean> getWithWaterMarkStream(SingleOutputStreamOperator<TrafficPageViewBean> beanStream) {
        return beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                    @Override
                    public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {
                        return trafficPageViewBean.getTs();
                    }
                }));
    }

    private SingleOutputStreamOperator<TrafficPageViewBean> getBeanStream(SingleOutputStreamOperator<JSONObject> jsonObjStream) {
        SingleOutputStreamOperator<TrafficPageViewBean> beanStream = jsonObjStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject object) throws Exception {
                return object.getJSONObject("common").getString("mid");
            }
//            三个参数：key的类型，输入流的类型，输出流的类型
        }).process(new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {
            ValueState<String> last_login_sdt;

            @Override
            public void open(Configuration parameters) throws Exception {
//                因为前面已经keyby完成了，所以这里是一个值的描述器，而值又是String类型
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last_login_dt", String.class);
//                设置状态的TTl为一天
                stateDescriptor
                        .enableTimeToLive(StateTtlConfig
                                .newBuilder(org.apache.flink.api.common.time.Time.days(1L))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build());

                last_login_sdt = getRuntimeContext().getState(stateDescriptor);

            }

            @Override
            public void processElement(JSONObject object,
                                       KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>.Context context,
                                       Collector<TrafficPageViewBean> collector) throws Exception {
//                判断是否独立访客
//                获取当前日期的时间
                Long ts = object.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                JSONObject common = object.getJSONObject("common");
                JSONObject page = object.getJSONObject("page");
//                声明独立访客的统计值
                long uvCt = 0L;
//                声明会话数的统计值
                long svCt = 0L;
//                获取状态中的日期
                String lastLoginDt = last_login_sdt.value();
//                如果状态没有存日期，或者两个日期时间不相等，那么则是独立访客
                if (lastLoginDt == null || !lastLoginDt.equals(curDt)) {
                    uvCt = 1L;
                } else {
                    last_login_sdt.update(curDt);
                }

//                判断会话数的方法：如果last_page为空，则是一个新的会话
                String last_page_id = object.getJSONObject("page").getString("last_page_id");
                if (last_page_id == null) {
                    svCt = 1L;
                }


                collector.collect(TrafficPageViewBean.builder()
                        .vc(common.getString("vc"))
                        .ar(common.getString("ar"))
                        .ch(common.getString("ch"))
                        .isNew(common.getString("is_new"))
                        .uvCt(uvCt)
                        .svCt(svCt)
                        .pvCt(1L)
                        .durSum(page.getLong("during_time"))
                        .sid(common.getString("sid"))
                        .ts(ts)
                        .build());

            }
        });
        return beanStream;
    }

    private SingleOutputStreamOperator<JSONObject> getJsonObjStream(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    Long ts = jsonObject.getLong("ts");
                    String mid = jsonObject.getJSONObject("common").getString("mid");
                    if (mid != null && ts != null) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("过滤脏数据" + s);
                }
            }
        });
    }
}
