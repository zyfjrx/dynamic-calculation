package com.byt.main;

import com.byt.calculate.DynamicProcessingTimeWindows;
import com.byt.calculate.func.*;
import com.byt.constants.PropertiesConstants;
import com.byt.func.*;
import com.byt.pojo.TagKafkaInfo;
import com.byt.utils.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @title: DWS动态计算
 * @author: zhangyifan
 * @date: 2022/8/29 16:51
 */
public class Dwd2DwsArithmeticJobTest {

    public static void main(String[] args) throws Exception {
        // TODO 0.获取执行环境和相关参数
        //StreamExecutionEnvironment env = StreamEnvUtil.getEnv("dwd2dws");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        OutputTag<TagKafkaInfo> avgOutPutTag = new OutputTag<TagKafkaInfo>("side-output-avg") {
        };
        OutputTag<TagKafkaInfo> lastOutPutTag = new OutputTag<TagKafkaInfo>("side-output-last") {
        };
        OutputTag<TagKafkaInfo> maxOutPutTag = new OutputTag<TagKafkaInfo>("side-output-max") {
        };
        OutputTag<TagKafkaInfo> minOutPutTag = new OutputTag<TagKafkaInfo>("side-output-min") {
        };
        OutputTag<TagKafkaInfo> MedianOutPutTag = new OutputTag<TagKafkaInfo>("side-output-median") {
        };
        OutputTag<TagKafkaInfo> dwdOutPutTag = new OutputTag<TagKafkaInfo>("side-output-dwd") {
        };
        SingleOutputStreamOperator<TagKafkaInfo> tagKafkaInfoDataStreamSource = env
                // 2.1 添加数据源
                .addSource(MyKafkaUtilDev.getKafkaPojoConsumerWM(
                        ConfigManager.getProperty("kafka.dwd.topic"),
                        "dwddws_" + System.currentTimeMillis())
                )
                .process(new ProcessFunction<TagKafkaInfo, TagKafkaInfo>() {
                    @Override
                    public void processElement(TagKafkaInfo value, ProcessFunction<TagKafkaInfo, TagKafkaInfo>.Context ctx, Collector<TagKafkaInfo> out) throws Exception {
                        if ("AVG".equals(value.getCurrCal())) {
                            ctx.output(avgOutPutTag, value);
                        } else if ("MAX".equals(value.getCurrCal())) {
                            ctx.output(maxOutPutTag, value);
                        } else if ("MIN".equals(value.getCurrCal())) {
                            ctx.output(minOutPutTag, value);
                        } else if ("MEDIAN".equals(value.getCurrCal())) {
                            ctx.output(MedianOutPutTag, value);
                        } else if ("LAST".equals(value.getCurrCal())) {
                            ctx.output(lastOutPutTag, value);
                        }
                    }
                });

        DataStream<TagKafkaInfo> avgDs = tagKafkaInfoDataStreamSource.getSideOutput(avgOutPutTag);
        DataStream<TagKafkaInfo> maxDs = tagKafkaInfoDataStreamSource.getSideOutput(maxOutPutTag);
        DataStream<TagKafkaInfo> minDs = tagKafkaInfoDataStreamSource.getSideOutput(minOutPutTag);
        DataStream<TagKafkaInfo> medianDs = tagKafkaInfoDataStreamSource.getSideOutput(MedianOutPutTag);
        DataStream<TagKafkaInfo> lastDs = tagKafkaInfoDataStreamSource.getSideOutput(lastOutPutTag);
        SingleOutputStreamOperator<TagKafkaInfo> resultAVGDS = avgDs.keyBy(r -> r.getBytName())
                .window(DynamicProcessingTimeWindows.of(Time.seconds(10L), Time.seconds(1L)))
                .process(new AvgProcessFunc(dwdOutPutTag));

        SingleOutputStreamOperator<TagKafkaInfo> resultLASTDS = lastDs
                .keyBy(r -> r.getBytName())
                .process(new LastProcessFunc(dwdOutPutTag));

        SingleOutputStreamOperator<TagKafkaInfo> resultMAXDS = maxDs.keyBy(r -> r.getBytName())
                .window(DynamicProcessingTimeWindows.of(Time.seconds(10L), Time.seconds(1L)))
                .process(new MaxProcessFunc(dwdOutPutTag));

        SingleOutputStreamOperator<TagKafkaInfo> resultMINDS = minDs.keyBy(r -> r.getBytName())
                .window(DynamicProcessingTimeWindows.of(Time.seconds(10L), Time.seconds(1L)))
                .process(new MinProcessFunc(dwdOutPutTag));

        SingleOutputStreamOperator<TagKafkaInfo> resultMEDIANDS = medianDs.keyBy(r -> r.getBytName())
                .window(DynamicProcessingTimeWindows.of(Time.seconds(10L), Time.seconds(1L)))
                .process(new MedianProcessFunc(dwdOutPutTag));

        // TODO 7.计算结果实时写入kafka

        DataStream<TagKafkaInfo> sideOutputAVG = resultAVGDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputMAX = resultMAXDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputMIN = resultMINDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputMEDIAN = resultMEDIANDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputLAST = resultLASTDS.getSideOutput(dwdOutPutTag);
        sideOutputAVG
                .union(sideOutputMAX, sideOutputMIN, sideOutputLAST,sideOutputMEDIAN)
                .map(new MapPojo2JsonStr<TagKafkaInfo>())
                .print("dwd>>>");
        sideOutputAVG
                .union(sideOutputMAX, sideOutputMIN, sideOutputLAST)
                .map(new MapPojo2JsonStr<TagKafkaInfo>())
                .addSink(MyKafkaUtilDev.getKafkaProducer(ConfigManager.getProperty(PropertiesConstants.KAFKA_DWD_TOPIC_PREFIX)));

        resultAVGDS.union(resultMAXDS, resultMINDS, resultLASTDS,resultMEDIANDS)
                .map(new MapPojo2JsonStr<TagKafkaInfo>())
                .print("dws:>");
        resultAVGDS.union(resultMAXDS, resultMINDS, resultLASTDS)
                .map(new MapPojo2JsonStr<TagKafkaInfo>())
                .addSink(MyKafkaUtilDev.getKafkaProducer(ConfigManager.getProperty(PropertiesConstants.KAFKA_DWS_TOPIC)));

        // TODO 8.启动任务
        env.execute("dws_arithmetic_line<");
    }

}
