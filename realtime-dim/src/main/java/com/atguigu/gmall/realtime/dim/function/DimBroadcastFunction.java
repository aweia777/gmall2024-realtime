package com.atguigu.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;

public class DimBroadcastFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {
    public HashMap<String,TableProcessDim> hashMap;
    public MapStateDescriptor<String, TableProcessDim> broadcastState;

    public DimBroadcastFunction(MapStateDescriptor<String, TableProcessDim> broadcastState) {
        this.broadcastState = broadcastState;
    }

    //                    预加载初始的维度表信息，为了避免主流可能跑得比广播流快的问题
    @Override
    public void open(Configuration parameters) throws Exception {
        java.sql.Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(mysqlConnection, "select * from gmall2023_config.table_process_dim", TableProcessDim.class,true );
        hashMap = new HashMap<>();
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            tableProcessDim.setOp("r");
            hashMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }
        JdbcUtil.closeConnection(mysqlConnection);
    }
    /**
     * 处理广播流数据
     * @param dim
     * @param context
     * @param collector
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(TableProcessDim dim,
                                        BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context,
                                        Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
//                读取广播流数据
        BroadcastState<String, TableProcessDim> tableProcessState = context.getBroadcastState(broadcastState);
//                将配置表中的信息作为状态，写入广播状态中作为标记
//                先获取cdc中的op
        String op = dim.getOp();
        if ("d".equals(op)) {
            tableProcessState.remove(dim.getSourceTable());
//                    同步删除hashmap中加载的配置表信息
            hashMap.remove(dim.getSourceTable());
        } else {
            tableProcessState.put(dim.getSourceTable(), dim);
        }
    }

    /**
     * 处理主流数据
     * @param object
     * @param readOnlyContext
     * @param collector
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject object,
                               BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext,
                               Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
//                读取广播状态
        ReadOnlyBroadcastState<String, TableProcessDim> tableProcessState = readOnlyContext.getBroadcastState(broadcastState);

//                使用主流中的数据来查询广播状态，检查是否匹配(这里是maxwell监控的流数据，格式会有所不同)
        String tableName = object.getString("table");

//                如果tableName的值能够被广播流获取到，说明tableName是一个广播流的表
        TableProcessDim tableProcessDim = tableProcessState.get(tableName);

//                如果是数据到的太早 造成状态为空
        if (tableProcessDim == null){
            tableProcessDim = hashMap.get(tableName);
        }

        if (tableProcessDim != null) {
//                    状态不为空，说明当前一行数据是维度表
            collector.collect(Tuple2.of(object, tableProcessDim));
        }
    }
}

