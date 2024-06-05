package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;

public class SQLUtil {
    public static String getKafkaSourceSQL(String TopicName,String groupId,String earliestOrlatest){
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '"+ TopicName + "',\n" +
                "  'properties.bootstrap.servers' = '"+ Constant.KAFKA_BROKERS+ "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = '"+ earliestOrlatest +  "-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    public static String getKafkaTopicDb(String groupId,String earliestOrlatest){
        return "CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "  `type` STRING,\n" +
                "  `data` MAP<STRING,STRING>,\n" +
                "  `old` MAP<STRING,STRING>,\n" +
                "  `proc_time` as PROCTIME(),\n" +
                "  `row_time`  as TO_TIMESTAMP_LTZ(ts * 1000,3),\n" +
                "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                ")" + getKafkaSourceSQL(Constant.TOPIC_DB,groupId,earliestOrlatest);
    }


    public static String getKafkaSinkSQL(String topicName){
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '"+ topicName + "',\n" +
                "  'properties.bootstrap.servers' = '"+ Constant.KAFKA_BROKERS+ "',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    /**
     * 获取upsert-kafka 时，一定要写不为空的主键
     * @param topicName
     * @return
     */
    public static String getUpsertKafkaSQl(String topicName){
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '"+ topicName + "',\n" +
                "  'properties.bootstrap.servers' = '"+ Constant.KAFKA_BROKERS+ "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }


}
