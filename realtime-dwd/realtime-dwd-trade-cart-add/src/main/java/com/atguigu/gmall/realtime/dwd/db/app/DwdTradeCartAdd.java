package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013, 4, "dwd_trade_cart_add");
    }


    @Override
    public void handle(StreamTableEnvironment tEnv, StreamExecutionEnvironment env, String ckAndGroupId) {
//        核心业务处理
//        读取topic_db
        createTopicDb(ckAndGroupId,tEnv);

//        2.筛选加购数据
        Table cartAddTable = filterCartAdd(tEnv);

//        3.创建kafka sink输出映射
        createKafkaSinkTable(tEnv);

//        4.写出筛选的数据到kafka的主题
        cartAddTable.execute().print();
//        cartAddTable.insertInto(Constant.TOPIC_DWD_TRADE_CART_ADD).execute();

    }

    public void createKafkaSinkTable(StreamTableEnvironment tEnv) {
        tEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_TRADE_CART_ADD + " (\n" +
                "    id STRING,\n" +
                "    user_id STRING,\n" +
                "    sku_id STRING,\n" +
                "    cart_price STRING,\n" +
                "    sku_num STRING,\n" +
                "    sku_name STRING,\n" +
                "    is_checked STRING,\n" +
                "    create_time STRING,\n" +
                "    operate_time STRING,\n" +
                "    is_ordered STRING,\n" +
                "    order_time STRING,\n" +
                "    source_type STRING,\n" +
                "    source_id STRING,\n" +
                "    ts BIGINT\n" +
                ")" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_CART_ADD));
    }

    public Table filterCartAdd(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery("select \n" +
                "`data`['id'] id,\n" +
                "`data`['user_id'] user_id,\n" +
                "`data`['sku_id'] sku_id,\n" +
                "`data`['cart_price'] cart_price,\n" +
                "if(`type` = 'insert',`data`['sku_num'],cast(cast(`data`['sku_num'] as BIGINT) - cast(`old`['sku_num'] as BIGINT) as STRING)) sku_num,\n" +
                "`data`['sku_name'] sku_name,\n" +
                "`data`['is_checked'] is_checked,\n" +
                "`data`['create_time'] create_time,\n" +
                "`data`['operate_time'] operate_time,\n" +
                "`data`['is_ordered'] is_ordered,\n" +
                "`data`['order_time'] order_time ,\n" +
                "`data`['source_type'] source_type ,\n" +
                "`data`['source_id'] source_id ,\n" +
                "`ts`\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'cart_info'\n" +
                "and (`type` = 'insert' or (`type` = 'update' and `old`['sku_num'] is not null \n" +
                "and cast(`data`['sku_num'] as BIGINT) > cast(`old`['sku_num'] as BIGINT) ) )");
    }
}
