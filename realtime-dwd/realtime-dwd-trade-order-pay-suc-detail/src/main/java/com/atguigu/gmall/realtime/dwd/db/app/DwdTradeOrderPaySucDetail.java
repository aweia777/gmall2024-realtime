package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderPaySucDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(10026, 4, "DwdTradeOrderPaySucDetail");
    }
    @Override
    public void handle(StreamTableEnvironment tEnv, StreamExecutionEnvironment env, String ckAndGroupId) {
//        核心业务逻辑
//        读取topic_db
        createTopicDb(ckAndGroupId, tEnv);

//        2.筛选支付成功的数据
        filterPaymentTable(tEnv);

//        3.读取下单详情表数据
        createOrderDetail(tEnv, ckAndGroupId);

//        4.创建base_dic
        createBaseDic(tEnv);

//        5.使用interval join连接两张表
        intervalJoin(tEnv);


//        6.使用lookup join完成维度退化
        Table join_table = getLookupJoin(tEnv);

//        7.创建kafka的写出，使用upsert-kafka
        createUpsertKafka(tEnv);

//        8.写出到kafka
        join_table.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS).execute();


    }

    private void createUpsertKafka(StreamTableEnvironment tEnv) {
        tEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS +"(\n" +
                "id STRING,\n" +
                "order_id STRING,\n" +
                "user_id STRING,\n" +
                "payment_type_code STRING,\n" +
                "payment_type_name STRING,\n" +
                "payment_time STRING ,\n" +
                "sku_id STRING,\n" +
                "province_id STRING,\n" +
                "activity_id STRING,\n" +
                "activity_rule_id STRING,\n" +
                "coupon_id STRING,\n" +
                "sku_name STRING,\n" +
                "order_price STRING,\n" +
                "sku_num STRING,\n" +
                "split_total_amount STRING,\n" +
                "split_activity_amount STRING,\n" +
                "split_coupon_amount STRING,\n" +
                "ts bigint,\n" +
                "PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getUpsertKafkaSQl(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));
    }

    private Table getLookupJoin(StreamTableEnvironment tEnv) {
        Table join_table = tEnv.sqlQuery("select\n" +
                "id,\n" +
                "order_id,\n" +
                "user_id,\n" +
                "payment_type payment_type_code,\n" +
                "info.dic_name payment_type_name,\n" +
                "payment_time ,\n" +
                "sku_id,\n" +
                "province_id,\n" +
                "activity_id,\n" +
                "activity_rule_id,\n" +
                "coupon_id,\n" +
                "sku_name,\n" +
                "order_price,\n" +
                "sku_num,\n" +
                "split_total_amount,\n" +
                "split_activity_amount,\n" +
                "split_coupon_amount,\n" +
                "ts\n" +
                "from payOrderTable p\n" +
                "left join base_dic FOR SYSTEM_TIME AS OF p.proc_time as b\n" +
                "on p.payment_type = b.rowkey");
        return join_table;
    }

    private void intervalJoin(StreamTableEnvironment tEnv) {
        Table payOrderTable = tEnv.sqlQuery("select\n" +
                "od.id,\n" +
                "p.order_id,\n" +
                "p.user_id,\n" +
                "payment_type,\n" +
                "callback_time payment_time ,\n" +
                "sku_id,\n" +
                "province_id,\n" +
                "activity_id,\n" +
                "activity_rule_id,\n" +
                "coupon_id,\n" +
                "sku_name,\n" +
                "order_price,\n" +
                "sku_num,\n" +
                "split_total_amount,\n" +
                "split_activity_amount,\n" +
                "split_coupon_amount,\n" +
                "p.ts,\n" +
                "p.proc_time\n" +
                "from payment_info p, order_detail od\n" +
                "where p.order_id = od.order_id\n" +
                "and p.row_time between od.row_time - INTERVAL '15' MINUTE and od.row_time + INTERVAL '5' MINUTE ");

        tEnv.createTemporaryView("payOrderTable", payOrderTable);
    }

    public void createOrderDetail(StreamTableEnvironment tEnv, String ckAndGroupId) {
        tEnv.executeSql("create table  order_detail (\n" +
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
                "row_time as TO_TIMESTAMP_LTZ(ts * 1000,3),\n" +
                "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, ckAndGroupId,"earliest"));
    }

    public void filterPaymentTable(StreamTableEnvironment tEnv) {
        Table payment_info = tEnv.sqlQuery("select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['payment_type'] payment_type,\n" +
                "    `data`['total_amount'] total_amount,\n" +
                "    `data`['callback_time'] callback_time,\n" +
                "     row_time,\n" +
                "     ts,\n" +
                "     proc_time\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'payment_info'\n" +
                "and `type` = 'update'\n" +
                "and `old`['payment_status'] is not null\n" +
                "and `data`['payment_status'] = '1602'");
        tEnv.createTemporaryView("payment_info", payment_info);
    }
}
