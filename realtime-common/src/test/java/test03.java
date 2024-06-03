import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class test03 {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(100L));

//         做执行操作
        tEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "  `data` MAP<STRING,STRING>,\n" +
                "  `old` MAP<STRING,STRING>,\n" +
                "  `proc_time` as PROCTIME()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_db',\n" +
                "  'properties.bootstrap.servers' = 'hadoop001:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
//        做查询操作
//        Table table = tEnv.sqlQuery("select * from topic_db where `database` = 'gmall' and `table`  = 'comment_info'");
//        table.execute().print();

        tEnv.executeSql("CREATE TABLE base_dic (\n" +
                "  dic_code STRING,\n" +
                "  dic_name STRING,\n" +
                "  parent_code STRING,\n" +
                "  PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://hadoop001:3306/gmall',\n" +
                "   'table-name' = 'base_dic',\n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456'\n" +
                ");");

//        tEnv.sqlQuery("select * from base_dic").execute().print();

        Table commen_info = tEnv.sqlQuery("select \n" +
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

        tEnv.createTemporaryView("comment_info", commen_info);

        tEnv.executeSql("select \n" +
                "id,\n" +
                "user_id,\n" +
                "nick_name,\n" +
                "head_img,\n" +
                "sku_id,\n" +
                "spu_id,\n" +
                "order_id,\n" +
                "appraise appraise_code,\n" +
                "dic_name appraise_name,\n" +
                "comment_txt,\n" +
                "create_time,\n" +
                "operate_time\n" +
                "from comment_info\n" +
                "join base_dic FOR SYSTEM_TIME AS OF comment_info.proc_time\n" +
                "on comment_info.appraise = base_dic.dic_code").print();







    }
}
