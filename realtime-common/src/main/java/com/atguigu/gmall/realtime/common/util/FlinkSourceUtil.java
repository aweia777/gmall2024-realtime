package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class FlinkSourceUtil {
    public static KafkaSource<String> getKafkaSource(String groupId,String topicName){
        return KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topicName)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setGroupId(groupId)
                .setValueOnlyDeserializer(
//                        new SimpleStringSchema()
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] bytes) throws IOException {
                                if(bytes != null && bytes.length != 0){
                                    return new String(bytes, StandardCharsets.UTF_8);
                                }
                                return "";
                            }

                            @Override
                            public boolean isEndOfStream(String s) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return BasicTypeInfo.STRING_TYPE_INFO;
                            }
                        }
                )
                .build();
    }

    public static MySqlSource<String> getMysqlSource(String database,String table){
        /***
         * 如果是8.0版本的mysql，加上下面两个参数.然后打开下面添加jdbc设置的代码
         *
         */
        Properties properties = new Properties();
        properties.setProperty("useSSL", "false");
        properties.setProperty("allowPublicKeyRetrieval", "true");



        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
//                .jdbcProperties(properties)
                .databaseList(database)
                //因为设计是多库多表，只写表名会有问题
                .tableList(database + "." + table)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();


        return mySqlSource;
    }
}
