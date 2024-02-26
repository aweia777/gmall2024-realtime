package com.atguigu.gmall.realtime.dim.app;

import com.atguigu.gmall.realtime.common.base.BaseApp;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DimAPP extends BaseApp {
    public static void main(String[] args) {
        new DimAPP().start(8390,4,"dimapp","topic_db");

    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream.print();
    }
}
