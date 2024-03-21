package com.byt.tagcalculate.ods;

import com.alibaba.fastjson.JSONObject;
import com.byt.common.utils.DorisUtil;
import com.byt.common.utils.EnvironmentUtils;
import com.byt.common.utils.MyKafkaUtil;
import com.byt.tagcalculate.func.BeanToJsonStrFunction;
import com.byt.tagcalculate.pojo.OdsTag;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;


/**
 * @title: 读取原始数据补充相关计算字段到DWD层
 * @author: zhangyf
 * @date: 2023/7/6 10:25
 */
public class Ods2Doris {
    public static void main(String[] args) throws Exception {
        // TODO 0.获取执行环境信息
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool parameterTool = EnvironmentUtils.createParameterTool();
        env.getConfig().setGlobalJobParameters(parameterTool);



        SingleOutputStreamOperator<String> kafkaDS = env
                .addSource(
                        MyKafkaUtil
                                .getKafkaStringConsumer("ods_data",
                                        "test1_20230808", parameterTool.get("kafka.server")
                                ));


        kafkaDS
                .addSink(DorisUtil.getDorisSink(
                        parameterTool.get("doris.fenode"),
                        parameterTool.get("doris.username"),
                        parameterTool.get("doris.password"),
                        "test_db.ods_tag")
                );

        kafkaDS.print();


        env.execute("dwd_dynamic_job");
    }
}