package com.atguigu.gmall.realtime.dwd.db.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class DwdBaseDb extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseDb().start(10020, 4, "DwdBaseDb", Constant.TOPIC_DB, OffsetsInitializer.earliest());
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
//        核心业务逻辑
//        1.读取topic_db
//        stream.print();

//        2.清洗过滤
        SingleOutputStreamOperator<JSONObject> jsonStream = flatmapTOjson(stream);

//        3.使用flinkCDC读取配置表
        DataStreamSource<String> table_process_dwd =
                env.fromSource(FlinkSourceUtil.getMysqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DWD_TABLE_NAME),
                        WatermarkStrategy.noWatermarks(),
                        "table_process_dwd").setParallelism(1);
//        table_process_dwd.print();

//        4.转换数据格式
        SingleOutputStreamOperator<TableProcessDwd> processDwdStream = flatMapToProcessDwd(table_process_dwd);

//        5.将配置表做成广播流
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<>(
                "process_state",
                String.class,
                TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastStream =
                processDwdStream.broadcast(mapStateDescriptor);

//        6.连接主流和广播流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processStream = processBaseDb(jsonStream, mapStateDescriptor, broadcastStream);

//        processStream.print();
//        筛选最后需要写出的字段
        SingleOutputStreamOperator<JSONObject> dataStreams = filterColumns(processStream);

//        dataStreams.print();
        dataStreams.sinkTo(KafkaSink.<JSONObject>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchema<JSONObject>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject object, KafkaSinkContext kafkaSinkContext, Long aLong) {
                            String topicName = object.getString("sink_table");
                            object.remove("sink_table");
                            return new ProducerRecord<byte[], byte[]>(topicName, Bytes.toBytes(object.toJSONString()));
                        }
                })
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("awei-" + "base_db" + new Random().nextLong())
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
                .build());
    }

    private SingleOutputStreamOperator<JSONObject> filterColumns(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processStream) {
        SingleOutputStreamOperator<JSONObject> dataStreams = processStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDwd>, JSONObject>() {
            @Override
            public JSONObject map(Tuple2<JSONObject, TableProcessDwd> value) throws Exception {
                JSONObject jsonObj = value.f0;
                TableProcessDwd processDwd = value.f1;
                JSONObject data = jsonObj.getJSONObject("data");
                List<String> columns = Arrays.asList(processDwd.getSinkColumns().split(","));
                data.keySet().removeIf(key -> !columns.contains(key));
                data.put("sink_table", processDwd.getSinkTable());
                return data;
            }
        });
        return dataStreams;
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processBaseDb(SingleOutputStreamOperator<JSONObject> jsonStream, MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor, BroadcastStream<TableProcessDwd> broadcastStream) {
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processStream = jsonStream.connect(broadcastStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {


                    HashMap<String, TableProcessDwd> hashMap = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Connection mysqlConnection = JdbcUtil.getMysqlConnection();
                        List<TableProcessDwd> tableProcessDwds = JdbcUtil.queryList(mysqlConnection, "select * from gmall2023_config.table_process_dwd", TableProcessDwd.class, true);
                        for (TableProcessDwd tableProcessDwd : tableProcessDwds) {
                            hashMap.put(tableProcessDwd.getSourceTable() + ":" + tableProcessDwd.getSourceType(), tableProcessDwd);
                        }
                    }

                    //                     将配置表的数据存到广播状态中
                    @Override
                    public void processBroadcastElement(TableProcessDwd tableProcessDwd,
                                                        BroadcastProcessFunction<JSONObject,
                                                                TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context,
                                                        Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
//                        使用context获取广播状态
                        BroadcastState<String, TableProcessDwd> broadcastState = context.getBroadcastState(mapStateDescriptor);
                        String op = tableProcessDwd.getOp();
                        String key = tableProcessDwd.getSourceTable() + ":" + tableProcessDwd.getSourceType();
                        if ("d".equals(op)) {
                            broadcastState.remove(key);
                            hashMap.remove(key);
                        } else {
                            broadcastState.put(key, tableProcessDwd);
                        }


//                        调用广播状态判断是否需要保留
                    }

                    @Override
                    public void processElement(JSONObject object,
                                               BroadcastProcessFunction<JSONObject, TableProcessDwd,
                                                       Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext readOnlyContext,
                                               Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
                        String table = object.getString("table");
                        String type = object.getString("type");
                        String key = table + ":" + type;

                        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
                        TableProcessDwd processDwd = broadcastState.get(key);

//                        二次判断是否为空
                        if (processDwd == null) {
                            processDwd = hashMap.get(key);
                        }

                        if (processDwd != null) {
                            collector.collect(Tuple2.of(object, processDwd));
                        }


                    }


                }).setParallelism(1);
        return processStream;
    }

    private SingleOutputStreamOperator<TableProcessDwd> flatMapToProcessDwd(DataStreamSource<String> table_process_dwd) {
        SingleOutputStreamOperator<TableProcessDwd> processDwdStream = table_process_dwd.flatMap(new FlatMapFunction<String, TableProcessDwd>() {
            @Override
            public void flatMap(String s, Collector<TableProcessDwd> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String op = jsonObject.getString("op");
                    TableProcessDwd tableProcessDwd;
//                如果op是D，说明是删除操作，数据都在before里面
                    if ("d".equals(op)) {
                        tableProcessDwd = jsonObject.getObject("before", TableProcessDwd.class);
                    } else {
                        tableProcessDwd = jsonObject.getObject("after", TableProcessDwd.class);
                    }
                    tableProcessDwd.setOp(op);
                    collector.collect(tableProcessDwd);
                } catch (Exception e) {
                    System.out.println("捕获脏数据" + s);
                }
            }
        }).setParallelism(1);
        return processDwdStream;
    }

    private SingleOutputStreamOperator<JSONObject> flatmapTOjson(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("清洗脏数据" + s);
                }
            }
        });
    }
}
