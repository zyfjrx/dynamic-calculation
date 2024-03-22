package com.byt.tagcalculate.ods;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.byt.common.cdc.FlinkCDC;
import com.byt.common.utils.EnvironmentUtils;
import com.byt.common.utils.MyKafkaUtil;
import com.byt.tagcalculate.func.BroadcastProcessFunc;
import com.byt.tagcalculate.pojo.OdsTag;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.tagcalculate.pojo.TagProperties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;


/**
 * @title: 读取kafka原始probuf协议数据转换为json数据
 * @author: zhangyf
 * @date: 2024/03/22 10:25
 */
public class Ods2JsonOds {
    public static void main(String[] args) throws Exception {
        // TODO 0.获取执行环境信息
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool parameterTool = EnvironmentUtils.createParameterTool();
        env.getConfig().setGlobalJobParameters(parameterTool);



        SingleOutputStreamOperator<String> kafkaDS = env
                .addSource(
                        MyKafkaUtil
                                .getKafkaListConsumer2(Arrays.asList(parameterTool.get("kafka.ods.topic").split(",")),
                                        "test1_20230808", parameterTool.get("kafka.server")
                                ))
                .flatMap(new FlatMapFunction<List<OdsTag>, String>() {
                    @Override
                    public void flatMap(List<OdsTag> odsTags, Collector<String> collector) throws Exception {
                        for (OdsTag odsTag : odsTags) {
                            SerializeConfig config = new SerializeConfig();
                            config.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
                            collector.collect(JSONObject.toJSONString(odsTag,config));
                        }
                    }
                });

        kafkaDS.addSink(MyKafkaUtil.getKafkaProducer("ods_data", parameterTool.get("kafka.server")))
                .name("sink to kafka");

        kafkaDS.print();

        env.execute("dwd_dynamic_job");
    }
}