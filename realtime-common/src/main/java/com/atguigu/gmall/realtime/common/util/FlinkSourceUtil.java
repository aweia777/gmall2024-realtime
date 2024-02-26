package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

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
}
