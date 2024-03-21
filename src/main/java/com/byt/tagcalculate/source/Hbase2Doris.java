package com.byt.tagcalculate.source;

import com.byt.common.utils.DorisUtil;
import com.byt.common.utils.EnvironmentUtils;
import com.byt.tagcalculate.func.BeanToJsonStrFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @title:
 * @author: zhangyf
 * @date: 2024/3/20 09:35
 **/
public class Hbase2Doris {
    public static void main(String[] args) throws Exception {
        // 获取执行环境和相关参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        System.setProperty("HADOOP_USER_NAME", "root");
        ParameterTool parameterTool = EnvironmentUtils.createParameterTool();
        env.getConfig().setGlobalJobParameters(parameterTool);

        SingleOutputStreamOperator<String> map = env.addSource(new HbaseSource())
                .map(new BeanToJsonStrFunction<>());
        map
                .addSink(DorisUtil.getDorisSink(
                        parameterTool.get("doris.fenode"),
                        parameterTool.get("doris.username"),
                        parameterTool.get("doris.password"),
                        "test_db.ods_tag")
                );

        map.print();
        env.execute();

    }
}
