package com.byt.tagcalculate.ods;

import com.byt.common.utils.EnvironmentUtils;
import com.byt.common.utils.MyKafkaUtil;
import com.byt.tagcalculate.func.BeanToJsonStrFunction;
import com.byt.tagcalculate.func.Ods2IoTDBFunc;
import com.byt.tagcalculate.pojo.OdsTagRecord;
import com.byt.tagcalculate.sink.IotDBSink3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.*;


/**
 * @title: 读取kafka原始数据到IoTDB --->ODS层
 * @author: zhangyf
 * @date: 2024/03/22 10:25
 */
public class Ods2IoTDB {
    public static void main(String[] args) throws Exception {
        // TODO 0.获取执行环境信息
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool parameterTool = EnvironmentUtils.createParameterTool();
        env.getConfig().setGlobalJobParameters(parameterTool);
        SingleOutputStreamOperator<OdsTagRecord> kafkaDS = env
                .addSource(
                        MyKafkaUtil
                                .getKafkaOdsListConsumer(
                                        Arrays.asList(parameterTool.get("kafka.ods.topic").split(",")),
                                        "test1_20240c324",
                                        parameterTool.get("kafka.server")
                                ))
                        .process(new Ods2IoTDBFunc());
        kafkaDS.map(new BeanToJsonStrFunction<>()).print();

       kafkaDS.addSink(new IotDBSink3());
        env.execute(Ods2IoTDB.class.getSimpleName());
    }
}