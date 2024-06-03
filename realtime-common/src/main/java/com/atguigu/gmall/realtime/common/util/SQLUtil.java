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
                "  `proc_time` as PROCTIME()\n" +
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


}
