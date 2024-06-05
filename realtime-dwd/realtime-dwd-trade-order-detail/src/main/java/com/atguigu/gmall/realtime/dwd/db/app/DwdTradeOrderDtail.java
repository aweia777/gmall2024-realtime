package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderDtail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderDtail().start(10024, 4, "dwd_trade_order_dtail");
    }
    @Override
    public void handle(StreamTableEnvironment tEnv, StreamExecutionEnvironment env, String ckAndGroupId) {
        
//        在flinkSQL中，join表的时候一定要添加状态的存活时间
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5L));
//        核心业务编写
//        读取kafka 的 topic_db
        createTopicDb(ckAndGroupId, tEnv);

//        2.筛选订单详情表
        filterOd(tEnv);

//        3.筛选订单信息表
        filterOi(tEnv);


//        4.筛选订单详情活动关联表
        filterOda(tEnv);

//        5.筛选订单详情优惠关联表
        filterOdc(tEnv);

//        6.将4张表格join合并
        Table join_table = getJoin_table(tEnv);

//        7.写出到kafka中,这里使用了left join，写入kafka的时候会产生撤回流，需要使用upsert kafka
        createUpsertKafkaSink(tEnv);

        join_table.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL).execute();

    }

    private void createUpsertKafkaSink(StreamTableEnvironment tEnv) {
        tEnv.executeSql("create table  " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL + " (\n" +
                "id String,\n" +
                "order_id String,\n" +
                "sku_id String,\n" +
                "user_id String,\n" +
                "province_id String,\n" +
                "activity_id String,\n" +
                "activity_rule_id String,\n" +
                "coupon_id String,\n" +
                "sku_name String,\n" +
                "order_price String,\n" +
                "sku_num String,\n" +
                "create_time String,\n" +
                "split_total_amount String,\n" +
                "split_activity_amount String,\n" +
                "split_coupon_amount String,\n" +
                "ts bigint,\n" +
                "PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getUpsertKafkaSQl(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
    }

    private void filterOdc(StreamTableEnvironment tEnv) {
        Table order_detail_coupon_table = tEnv.sqlQuery("select \n" +
                "    `data`['order_detail_id'] id,\n" +
                "    `data`['coupon_id'] coupon_id\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'order_detail_coupon'\n" +
                "and `type` = 'insert'");
        tEnv.createTemporaryView("order_detail_coupon_table", order_detail_coupon_table);
    }

    public void filterOda(StreamTableEnvironment tEnv) {
        Table order_detail_activity_table = tEnv.sqlQuery("select \n" +
                "    `data`['order_detail_id'] id,\n" +
                "    `data`['activity_id'] activity_id,\n" +
                "    `data`['activity_rule_id'] activity_rule_id\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'order_detail_activity'\n" +
                "and `type` = 'insert'");
        tEnv.createTemporaryView("order_detail_activity_table", order_detail_activity_table);
    }

    public void filterOi(StreamTableEnvironment tEnv) {
        Table order_info_table = tEnv.sqlQuery("select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['province_id'] province_id,\n" +
                "    `ts`\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'order_info'\n" +
                "and `type` = 'insert'");
        tEnv.createTemporaryView("order_info_table", order_info_table);
    }

    public void filterOd(StreamTableEnvironment tEnv) {
        Table order_detail_table = tEnv.sqlQuery("select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['sku_name'] sku_name,\n" +
                "    `data`['order_price'] order_price,\n" +
                "    `data`['sku_num'] sku_num,\n" +
                "    `data`['create_time'] create_time,\n" +
                "    `data`['split_total_amount'] split_total_amount,\n" +
                "    `data`['split_activity_amount'] split_activity_amount,\n" +
                "    `data`['split_coupon_amount'] split_coupon_amount,\n" +
                "    `ts`\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'order_detail'\n" +
                "and `type` = 'insert'");
        tEnv.createTemporaryView("order_detail_table",order_detail_table);
    }

    public Table getJoin_table(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery("select \n" +
                "od.id,\n" +
                "order_id,\n" +
                "sku_id,\n" +
                "user_id,\n" +
                "province_id,\n" +
                "activity_id, \n" +
                "activity_rule_id, \n" +
                "coupon_id, \n" +
                "sku_name,\n" +
                "order_price,\n" +
                "sku_num,\n" +
                "create_time,\n" +
                "split_total_amount,\n" +
                "split_activity_amount,\n" +
                "split_coupon_amount,\n" +
                "od.ts\n" +
                "from\n" +
                "order_detail_table od\n" +
                "join\n" +
                "order_info_table oi\n" +
                "on od.order_id = oi.id\n" +
                "left join order_detail_activity_table oda\n" +
                "on oda.id = od.id\n" +
                "left join order_detail_coupon_table odc\n" +
                "on odc.id = od.id");
    }
}
