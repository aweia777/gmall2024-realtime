package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
        tEnv.executeSql("select \n" +
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
                "`ts`\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'cart_info'\n" +
                "and (`type` = 'insert' or (`type` = 'update' and `old`['sku_num'] is not null \n" +
                "and cast(`data`['sku_num'] as BIGINT) > cast(`old`['sku_num'] as BIGINT) ) )").print();
    }
}
