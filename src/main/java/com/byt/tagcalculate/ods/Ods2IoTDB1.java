package com.byt.tagcalculate.ods;

import com.byt.common.utils.EnvironmentUtils;
import com.byt.common.utils.MyKafkaUtil;
import com.byt.common.utils.TimeUtil;
import com.byt.tagcalculate.func.BeanToJsonStrFunction;
import com.byt.tagcalculate.pojo.Event;
import com.byt.tagcalculate.pojo.OdsTag;
import com.byt.tagcalculate.pojo.OdsTags;
import com.byt.tagcalculate.sink.IotDBSink2;
import com.byt.tagcalculate.sink.IotDBSink3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * @title: 读取kafka原始数据到IoTDB --->ODS层
 * @author: zhangyf
 * @date: 2024/03/22 10:25
 */
public class Ods2IoTDB1 {
    public static void main(String[] args) throws Exception {
        // TODO 0.获取执行环境信息
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool parameterTool = EnvironmentUtils.createParameterTool();
        env.getConfig().setGlobalJobParameters(parameterTool);

        DataStreamSource<Event> kafkaDS = env
                .addSource(
                        MyKafkaUtil
                                .getKafkaEventTagsConsumer(
                                        Arrays.asList(parameterTool.get("kafka.ods.topic").split(",")),
                                        "test1_202c40c324",
                                        parameterTool.get("kafka.server")
                                ));
        kafkaDS.map(new BeanToJsonStrFunction<>()).print();
        kafkaDS.addSink(new IotDBSink2());
        env.execute(Ods2IoTDB1.class.getSimpleName());
    }
}