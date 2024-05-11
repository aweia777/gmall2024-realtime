package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseUtil {

    public static Connection getConnection() {
//        方式一：使用configuration
//        Configuration conf = new Configuration();
//        conf.set("hbase.zookeeper.quorum", Constant.HBASE_ZOOKEEPER_QUORUM);
//        使用配置文件
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static void closeConnection(Connection conn) throws IOException {
        if (conn != null && !conn.isClosed()){
            conn.close();
        }
    }
}
