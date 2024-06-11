package com.atguigu.gmall.realtime.dws.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import function.KwSplit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(10031,4,"DwsTrafficSourceKeywordPageViewWindow");
    }
    @Override
    public void handle(StreamTableEnvironment tEnv, StreamExecutionEnvironment env, String ckAndGroupId) {
//        核心业务处理
//        1.读取主流的数据，dwd的页面主题数据
        createPageInfo(tEnv, ckAndGroupId);

//        2.筛选关键字keywords
        filterKeyWords(tEnv);

//        3.自定UDTF函数,并注册
        tEnv.createTemporaryFunction("split_function", KwSplit.class);

//        4.调用分词函数进行拆分
        keyWordSplit(tEnv);

//        5.对keyword进行分窗聚合
        Table sinkTable = getWindowAggTable(tEnv);
//        sinkTable.execute().print();

//        6.写出到doris
        createDorisSinkTable(tEnv);

        sinkTable.insertInto("sinkDorisTable").execute();




    }

    private void createDorisSinkTable(StreamTableEnvironment tEnv) {
        tEnv.executeSql(" create table sinkDorisTable(\n" +
                " \tstt STRING,\n" +
                " \tedt STRING,\n" +
                " \tcur_date STRING,\n" +
                " \tkeyword STRING,\n" +
                " \tkeyword_count bigint\n" +
                " )" + SQLUtil.getSinkDorisSQL(Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW));
    }

    private Table getWindowAggTable(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery("SELECT \n" +
                "cast(TUMBLE_START(row_time,INTERVAL '10' second) as STRING) as stt,\n" +
                "cast(TUMBLE_END(row_time,INTERVAL '10' second)as STRING) as edt,\n" +
                "cast(CURRENT_DATE as STRING) as cur_date,\n" +
                "keyword,\n" +
                "count(*) keyword_count\n" +
                "  FROM keyword_table\n" +
                "  GROUP BY\n" +
                "    TUMBLE(row_time, INTERVAL '10' second),\n" +
                " keyword");
    }

    private void keyWordSplit(StreamTableEnvironment tEnv) {
        Table keywordTable = tEnv.sqlQuery("select keywords,keyword,length,row_time\n" +
                "from keywords_table\n" +
                "left join LATERAL TABLE(split_function(keywords)) ON TRUE");
        tEnv.createTemporaryView("keyword_table", keywordTable);
    }

    private void filterKeyWords(StreamTableEnvironment tEnv) {
        Table keywordsTable = tEnv.sqlQuery("select\n" +
                "\tpage['item'] keywords,\n" +
                "\trow_time\n" +
                "from\n" +
                "page_info\n" +
                "where page['last_page_id'] = 'search'\n" +
                "and page['item_type'] = 'keyword'\n" +
                "and page['item'] is not null");
        tEnv.createTemporaryView("keywords_table", keywordsTable);
    }

    public void createPageInfo(StreamTableEnvironment tEnv, String ckAndGroupId) {
        tEnv.executeSql("create table page_info(\n" +
                "\t`common` map<STRING,STRING>,\n" +
                "\t`page` map<STRING,STRING>,\n" +
                "\t`ts` bigint,\n" +
                "\t`row_time` as TO_TIMESTAMP_LTZ(ts,3),\n" +
                "\tWATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND \n" +
                ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRAFFIC_PAGE, ckAndGroupId,"earliest"));
    }
}
