package com.byt.main;

import com.byt.calculate.DynamicSlidingEventTimeWindows;
import com.byt.calculate.func.*;
import com.byt.constants.PropertiesConstants;
import com.byt.func.*;
import com.byt.pojo.TagKafkaInfo;
import com.byt.utils.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.HashMap;

/**
 * @title: DWS动态计算
 * @author: zhangyifan
 * @date: 2022/8/29 16:51
 */
public class Dwd2DwsArithmeticJobTest {

    public static void main(String[] args) throws Exception {
        // TODO 0.获取执行环境和相关参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        HashMap<String, OutputTag<TagKafkaInfo>> sideOutPutTags = SideOutPutTagUtil.getSideOutPutTags();

        OutputTag<TagKafkaInfo> dwdOutPutTag = new OutputTag<TagKafkaInfo>("side-output-dwd") {
        };
        SingleOutputStreamOperator<TagKafkaInfo> tagKafkaInfoDataStreamSource = env
                // 2.1 添加数据源
                .addSource(MyKafkaUtilDev.getKafkaPojoConsumerWM(
                        ConfigManager.getProperty("kafka.dwd.topic"),
                        "dwddws_" + System.currentTimeMillis())
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TagKafkaInfo>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                                .withTimestampAssigner(new SerializableTimestampAssigner<TagKafkaInfo>() {
                                    @Override
                                    public long extractTimestamp(TagKafkaInfo tagKafkaInfo, long l) {
                                        return tagKafkaInfo.getTimestamp();
                                    }
                                })
                )
                .process(new ProcessFunction<TagKafkaInfo, TagKafkaInfo>() {
                    @Override
                    public void processElement(TagKafkaInfo value, ProcessFunction<TagKafkaInfo, TagKafkaInfo>.Context ctx, Collector<TagKafkaInfo> out) throws Exception {
                        if (sideOutPutTags.containsKey(value.getCurrCal())) {
                            ctx.output(sideOutPutTags.get(value.getCurrCal()), value);
                        }
                    }
                });

        DataStream<TagKafkaInfo> avgDs = tagKafkaInfoDataStreamSource.getSideOutput(sideOutPutTags.get(PropertiesConstants.AVG));
        DataStream<TagKafkaInfo> maxDs = tagKafkaInfoDataStreamSource.getSideOutput(sideOutPutTags.get(PropertiesConstants.MAX));
        DataStream<TagKafkaInfo> minDs = tagKafkaInfoDataStreamSource.getSideOutput(sideOutPutTags.get(PropertiesConstants.MIN));
        DataStream<TagKafkaInfo> medianDs = tagKafkaInfoDataStreamSource.getSideOutput(sideOutPutTags.get(PropertiesConstants.MEDIAN));
        DataStream<TagKafkaInfo> cvDs = tagKafkaInfoDataStreamSource.getSideOutput(sideOutPutTags.get(PropertiesConstants.CV));
        DataStream<TagKafkaInfo> dejumpDs = tagKafkaInfoDataStreamSource.getSideOutput(sideOutPutTags.get(PropertiesConstants.DEJUMP));
        DataStream<TagKafkaInfo> fofDs = tagKafkaInfoDataStreamSource.getSideOutput(sideOutPutTags.get(PropertiesConstants.FOF));
        DataStream<TagKafkaInfo> interpDs = tagKafkaInfoDataStreamSource.getSideOutput(sideOutPutTags.get(PropertiesConstants.INTERP));
        DataStream<TagKafkaInfo> lastDs = tagKafkaInfoDataStreamSource.getSideOutput(sideOutPutTags.get(PropertiesConstants.LAST));
        DataStream<TagKafkaInfo> trendDs = tagKafkaInfoDataStreamSource.getSideOutput(sideOutPutTags.get(PropertiesConstants.TREND));
        DataStream<TagKafkaInfo> varDs = tagKafkaInfoDataStreamSource.getSideOutput(sideOutPutTags.get(PropertiesConstants.VAR));

        SingleOutputStreamOperator<TagKafkaInfo> resultAVGDS = avgDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of())
                .process(new AvgProcessFunc(dwdOutPutTag));

        SingleOutputStreamOperator<TagKafkaInfo> resultLASTDS = lastDs.keyBy(r -> r.getBytName())
                .process(new LastProcessFunc(dwdOutPutTag));

        SingleOutputStreamOperator<TagKafkaInfo> resultMAXDS = maxDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of())
                .process(new MaxProcessFunc(dwdOutPutTag));

        SingleOutputStreamOperator<TagKafkaInfo> resultMINDS = minDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of())
                .process(new MinProcessFunc(dwdOutPutTag));

        SingleOutputStreamOperator<TagKafkaInfo> resultMEDIANDS = medianDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of())
                .process(new MedianProcessFunc(dwdOutPutTag));

        SingleOutputStreamOperator<TagKafkaInfo> resultCVDS = cvDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of())
                .process(new CvProcessFunc(dwdOutPutTag));

        SingleOutputStreamOperator<TagKafkaInfo> resultDEJUMP = dejumpDs.keyBy(r -> r.getBytName())
                .process(new DejumpProcessFunc(dwdOutPutTag));

        SingleOutputStreamOperator<TagKafkaInfo> resultFOFDS = fofDs.keyBy(r -> r.getBytName())
                .process(new FofProcessFunc(dwdOutPutTag));

        SingleOutputStreamOperator<TagKafkaInfo> resultINTERPDS = interpDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of())
                .process(new InterpProcessFunc(dwdOutPutTag));

        SingleOutputStreamOperator<TagKafkaInfo> resultTRENDDS = trendDs.keyBy(r -> r.getBytName())
                .process(new TrendProcessFunc(dwdOutPutTag));

        SingleOutputStreamOperator<TagKafkaInfo> resultVARDS = trendDs.keyBy(r -> r.getBytName())
                .process(new VarProcessFunc(dwdOutPutTag));


        // 获取还需进一步计算的数据
        DataStream<TagKafkaInfo> sideOutputAVG = resultAVGDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputMAX = resultMAXDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputMIN = resultMINDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputMEDIAN = resultMEDIANDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputLAST = resultLASTDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputCV = resultCVDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputDEJUMP = resultDEJUMP.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputFOF = resultFOFDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputTREND = resultTRENDDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputVAR = resultVARDS.getSideOutput(dwdOutPutTag);
        sideOutputAVG
                .union(sideOutputMAX, sideOutputMIN, sideOutputLAST, sideOutputMEDIAN,sideOutputCV,sideOutputDEJUMP,sideOutputFOF,sideOutputTREND,sideOutputVAR)
                .map(new MapPojo2JsonStr<TagKafkaInfo>())
                .print("dwd>>>");
        sideOutputAVG
                .union(sideOutputMAX, sideOutputMIN, sideOutputLAST)
                .map(new MapPojo2JsonStr<TagKafkaInfo>())
                .addSink(MyKafkaUtilDev.getKafkaProducer(ConfigManager.getProperty(PropertiesConstants.KAFKA_DWD_TOPIC_PREFIX)));

        // union计算完成数据
        resultAVGDS
                .union(resultMAXDS, resultMINDS, resultLASTDS, resultMEDIANDS,resultCVDS,resultDEJUMP,resultFOFDS,resultTRENDDS,resultVARDS)
                .map(new MapPojo2JsonStr<TagKafkaInfo>())
                .print("dws:>");
        resultAVGDS.union(resultMAXDS, resultMINDS, resultLASTDS)
                .map(new MapPojo2JsonStr<TagKafkaInfo>())
                .addSink(MyKafkaUtilDev.getKafkaProducer(ConfigManager.getProperty(PropertiesConstants.KAFKA_DWS_TOPIC)));

        // TODO 8.启动任务
        env.execute("dws_arithmetic_line<");
    }

}
