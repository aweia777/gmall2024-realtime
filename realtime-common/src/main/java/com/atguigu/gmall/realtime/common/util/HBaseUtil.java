package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUtil {
    /**
     * 获取HBase连接
     * @return 一个HBase的同步连接
     */
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

    /**
     * 关闭Hbase的连接
     * @param conn 一个HBase的同步连接
     * @throws IOException 连接异常
     */
    public static void closeConnection(Connection conn) throws IOException {
        if (conn != null && !conn.isClosed()){
            conn.close();
        }
    }

    /**
     * 创建表格
     * @param conn 一个HBase的同步连接
     * @param namespace 命名空间
     * @param table 表名
     * @param familes 列族
     * @throws IOException admin连接异常
     */
    public static void createTable(Connection conn,String namespace,String table,String... familes) throws IOException {
        if (familes == null || familes.length == 0 ){
            System.out.println("列族不能为空");
            return;
        }

//        1.获取admin
        Admin admin = conn.getAdmin();
//        2.创建表格描述
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, table));
//        3.创建列族描述，有多个列，需要使用for循环来遍历添加到表格描述中
        for (String famile : familes) {
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(famile)).build();
            tableDescriptorBuilder.setColumnFamily(familyDescriptor);
        }
//        4.使用admin调用createTable
        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
//        5.使用admin来关闭连接，这里不要关闭connection
        admin.close();

    }

    /**
     * 删除表格
     * @param conn 一个HBase的同步连接
     * @param namespace 命名空间
     * @param table 表名
     * @throws IOException admin连接异常
     */
    public static void deleteTable(Connection conn,String namespace,String table) throws IOException {
        Admin admin = conn.getAdmin();

        try {
            admin.disableTable(TableName.valueOf(namespace,table));
            admin.deleteTable(TableName.valueOf(namespace,table));
        } catch (IOException e) {
            e.printStackTrace();
        }

        admin.close();
    }
}
