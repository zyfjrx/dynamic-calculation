package com.byt.tagcalculate.ods;

import com.byt.common.utils.EnvironmentUtils;
import com.byt.common.utils.MyKafkaUtil;
import com.byt.common.utils.TimeUtil;
import com.byt.tagcalculate.func.BeanToJsonStrFunction;
import com.byt.tagcalculate.pojo.OdsTag;
import com.byt.tagcalculate.pojo.OdsTags;
import com.byt.tagcalculate.sink.IotDBSink3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

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
        OutputTag<List<OdsTag>> halfOutPut = new OutputTag<List<OdsTag>>("side-output-dwd") {
        };
        SingleOutputStreamOperator<OdsTags> kafkaDS = env
                .addSource(
                        MyKafkaUtil
                                .getKafkaOdsListConsumer(
                                        Arrays.asList(parameterTool.get("kafka.ods.topic").split(",")),
                                        "test1_20240c324",
                                        parameterTool.get("kafka.server")
                                ))
                .process(new ProcessFunction<List<OdsTag>, OdsTags>() {
                    @Override
                    public void processElement(List<OdsTag> odsTagList, ProcessFunction<List<OdsTag>, OdsTags>.Context context, Collector<OdsTags> collector) throws Exception {
                        OdsTags odsTags = new OdsTags();

                        List<Long> timestamps = new ArrayList<>();
                        List<List<String>> measurementsList = new ArrayList<>();
                        List<List<TSDataType>> typesList = new ArrayList<>();
                        List<List<Object>> valuesList = new ArrayList<>();
                        for (OdsTag odsTag : odsTagList) {
                            if (odsTag.getTagName().contains("/")) {
                                String tagName = odsTag.getTagName();
                                String replace = tagName.replace("/", "_");
                                odsTag.setTagName(replace);
                            }
                            measurementsList.add(Collections.singletonList(odsTag.getTagName()));
                            valuesList.add(Collections.singletonList(odsTag.getValue()));
                            typesList.add(Collections.singletonList(TSDataType.DOUBLE));
                            timestamps.add(TimeUtil.getStartTime(odsTag.getTime()));
                        }
                        odsTags.setTagNames(measurementsList);
                        odsTags.setTimestamps(timestamps);
                        odsTags.setValues(valuesList);
                        odsTags.setTypes(typesList);
                        collector.collect(odsTags);
                    }
                })
                ;
        kafkaDS.map(new BeanToJsonStrFunction<>()).print();

       kafkaDS.addSink(new IotDBSink3());
        env.execute(Ods2IoTDB.class.getSimpleName());
    }
}