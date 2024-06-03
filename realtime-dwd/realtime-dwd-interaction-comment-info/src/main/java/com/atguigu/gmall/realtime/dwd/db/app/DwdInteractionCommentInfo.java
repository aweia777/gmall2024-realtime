package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012, 4, "dwd_interaction_comment_info");
    }
    @Override
    public void handle(StreamTableEnvironment tEnv, StreamExecutionEnvironment env, String ckAndGroupId) {
//        核心逻辑
//        读取topic_db
        createTopicDb(ckAndGroupId,tEnv);

//        读取base_dic
        createBaseDic(tEnv);

//        筛选topic_db，过滤出评论信息表的数据
        filterCommentInfo(tEnv);


//        使用lookup join完成维度退化
        Table joinTable = lookUpJoin(tEnv);

//        创建kafka sink对应的表格
        createSinkTable(tEnv);

//        写出到对应的kafka主题
        joinTable.insertInto(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO).execute();


//
    }

    public void createSinkTable(StreamTableEnvironment tEnv) {
        tEnv.executeSql("create table dwd_interaction_comment_info(" +
                "id  STRING,\n" +
                "user_id STRING, \n" +
                "nick_name STRING, \n" +
                "sku_id STRING, \n" +
                "spu_id STRING, \n" +
                "order_id STRING, \n" +
                "appraise_code STRING, \n" +
                "appraise_name STRING, \n" +
                "comment_txt STRING, \n" +
                "create_time STRING, \n" +
                "operate_time STRING " +
                ")"
                + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
    }

    public Table lookUpJoin(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery("select \n" +
                "id,\n" +
                "user_id,\n" +
                "nick_name,\n" +
                "sku_id,\n" +
                "spu_id,\n" +
                "order_id,\n" +
                "appraise appraise_code,\n" +
                "info.dic_name appraise_name,\n" +
                "comment_txt,\n" +
                "create_time,\n" +
                "operate_time\n" +
                "from comment_info\n" +
                "join base_dic FOR SYSTEM_TIME AS OF comment_info.proc_time\n" +
                "on comment_info.appraise = base_dic.rowkey");
    }

    public void filterCommentInfo(StreamTableEnvironment tEnv) {
        Table commentInfo = tEnv.sqlQuery("select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['nick_name'] nick_name,\n" +
                "    `data`['head_img'] head_img,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['spu_id'] spu_id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['appraise'] appraise,\n" +
                "    `data`['comment_txt'] comment_txt,\n" +
                "    `data`['create_time'] create_time,\n" +
                "    `data`['operate_time'] operate_time,\n" +
                "    `proc_time`\n" +
                " from topic_db  \n" +
                " where `database` = 'gmall' \n" +
                " and `table` = 'comment_info' \n" +
                " and `type` = 'insert'");

        tEnv.createTemporaryView("comment_info", commentInfo);
    }

}
