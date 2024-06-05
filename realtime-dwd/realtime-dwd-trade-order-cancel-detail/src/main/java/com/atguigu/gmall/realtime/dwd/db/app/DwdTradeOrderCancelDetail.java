package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderCancelDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderCancelDetail().start(10025, 5, "dwd_trade_order_cancel-detail");
    }
    @Override
    public void handle(StreamTableEnvironment tEnv, StreamExecutionEnvironment env, String ckAndGroupId) {
//        设置ttl为15 * 60 + 5 seconds
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds( 15 * 60 + 5L));

//        读取topic_db
        createTopicDb(ckAndGroupId, tEnv);

//        以订单明细表为主表
        Table od_table = tEnv.sqlQuery("select \n" +
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
        tEnv.createTemporaryView("order_detail_table",od_table);

//        订单表
        Table order_cancel_table = tEnv.sqlQuery("select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['province_id'] province_id,\n" +
                "    `data`['operate_time'] operate_time,\n" +
                "    `ts`\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'order_info'\n" +
                "and `type` = 'update'\n" +
                "and `old`['order_status'] = '1001'\n" +
                "and `data`['order_status'] = '1003'");

        tEnv.createTemporaryView("order_cancel_table", order_cancel_table);

//        tEnv.sqlQuery("select * from order_cancel_table oc join order_detail_table od on oc.id = od.order_id").execute().print();

//        订单明细活动表
        Table order_detail_activity_table = tEnv.sqlQuery("select \n" +
                "    `data`['order_detail_id'] id,\n" +
                "    `data`['activity_id'] activity_id,\n" +
                "    `data`['activity_rule_id'] activity_rule_id\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'order_detail_activity'\n" +
                "and `type` = 'insert'");
        tEnv.createTemporaryView("order_detail_activity_table", order_detail_activity_table);

//        订单明细优惠券表
        Table order_detail_coupon_table = tEnv.sqlQuery("select \n" +
                "    `data`['order_detail_id'] id,\n" +
                "    `data`['coupon_id'] coupon_id\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'order_detail_coupon'\n" +
                "and `type` = 'insert'");
        tEnv.createTemporaryView("order_detail_coupon_table", order_detail_coupon_table);

//        join四张表
        Table joinTable = tEnv.sqlQuery("select\n" +
                "    od.id,\n" +
                "    od.order_id,\n" +
                "    od.sku_id,\n" +
                "    user_id,\n" +
                "    province_id,\n" +
                "    activity_id, \n" +
                "    activity_rule_id, \n" +
                "    coupon_id, \n" +
                "    oct.operate_time,\n" +
                "    sku_name,\n" +
                "    order_price,\n" +
                "    sku_num,\n" +
                "    create_time,\n" +
                "    split_total_amount,\n" +
                "    split_activity_amount,\n" +
                "    split_coupon_amount,\n" +
                "    oct.ts\n" +
                "from\n" +
                "order_cancel_table oct\n" +
                "join\n" +
                "order_detail_table od\n" +
                "on oct.id = od.order_id\n" +
                "left join\n" +
                "order_detail_activity_table oda\n" +
                "on oct.id = oda.id\n" +
                "left join\n" +
                "order_detail_coupon_table odc\n" +
                "on oct.id = odc.id");

        tEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_CANCEL + " (\n" +
                "    id STRING,\n" +
                "    order_id STRING,\n" +
                "    sku_id STRING,\n" +
                "    user_id STRING,\n" +
                "    province_id STRING,\n" +
                "    activity_id STRING, \n" +
                "    activity_rule_id STRING, \n" +
                "    coupon_id STRING, \n" +
                "    operate_time STRING,\n" +
                "    sku_name STRING,\n" +
                "    order_price STRING,\n" +
                "    sku_num STRING,\n" +
                "    create_time STRING,\n" +
                "    split_total_amount STRING,\n" +
                "    split_activity_amount STRING,\n" +
                "    split_coupon_amount STRING,\n" +
                "    ts BIGINT,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ")"    + SQLUtil.getUpsertKafkaSQl(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));


        joinTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL).execute();

    }
}
