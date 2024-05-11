package com.atguigu.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DimAPP extends BaseApp {
    public static void main(String[] args) {
        new DimAPP().start(10001,4,"dimapp-1", Constant.TOPIC_DB);

    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
//        核心业务逻辑
/**
 *         1.对ods层的数据进行清洗(使用filter加map)
 *             stream.filter(new FilterFunction<String>() {
 *                 @Override
 *                 public boolean filter(String s) throws Exception {
 *                     boolean flat = false;
 *                     try {
 *                         JSONObject jsonObject = JSONObject.parseObject(s);
 *                         String database = jsonObject.getString("database");
 *                         String type = jsonObject.getString("type");
 *                         JSONObject data = jsonObject.getJSONObject("data");
 *                         if ("gmall".equals(database) && !"bootstrap-start".equals(type)
 *                          && !"bootstrap-complete".equals(type)
 *                                 && data != null && data.size() != 0
 *                         ) {
 *                             flat = true;
 *                         }
 *                     }
 *                     catch (Exception e){
 *                         e.printStackTrace();
 *                     }
 *                     return flat;
 *                 }
 *             }).map(new MapFunction<String, JSONObject>() {
 *                 @Override
 *                 public JSONObject map(String s) throws Exception {
 *                     return JSONObject.parseObject(s);
 *                 }
 *             });
 */

//      1.对ods层的数据进行清洗(使用flatmap)
        SingleOutputStreamOperator<JSONObject> etl = etl(stream);
//      2.使用flinkcdc监控mysql的日志变化
        MySqlSource<String> mysqlSource = FlinkSourceUtil.getMysqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DIM_TABLE_NAME);
        //读取mysql的binlog需要将并行度设置为1，否则会有bug捕捉不到变更
        DataStreamSource<String> mysqlsource = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysqlsource").setParallelism(1);
//        mysqlsource.print();
//        3.在HBASE里面创建维度表

    }

    public SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream){
        return stream.flatMap((FlatMapFunction<String, JSONObject>) (s, collector) -> {
            try {
                JSONObject jsonObject = JSONObject.parseObject(s);
                String database = jsonObject.getString("database");
                String type = jsonObject.getString("type");
                JSONObject data = jsonObject.getJSONObject("data");
                if ("gmall".equals(database) && !"bootstrap-start".equals(type)
                        && !"bootstrap-complete".equals(type)
                        && data != null && data.size() != 0
                ) {
                    collector.collect(jsonObject);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).returns(JSONObject.class);
    }

}
