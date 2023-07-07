package com.byt.main;

import com.byt.calculate.DynamicSlidingEventTimeWindows;
import com.byt.calculate.func.*;
import com.byt.constants.PropertiesConstants;
import com.byt.func.*;
import com.byt.pojo.TagKafkaInfo;
import com.byt.sink.DbResultBatchSink;
import com.byt.utils.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * @title: DWS动态计算
 * @author: zhangyifan
 * @date: 2022/8/29 16:51
 */
public class Dwd2DwsDynamicCalculationJob {

    public static void main(String[] args) throws Exception {
        // TODO 0.获取执行环境和相关参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        HashMap<String, OutputTag<TagKafkaInfo>> sideOutPutTags = SideOutPutTagUtil.getSideOutPutTags();
        Set<String> strings = sideOutPutTags.keySet();
        for (String string : strings) {
            System.out.println(string + "------------------>" + sideOutPutTags.get(string));
        }

        OutputTag<TagKafkaInfo> dwdOutPutTag = new OutputTag<TagKafkaInfo>("side-output-dwd") {
        };
        OutputTag<TagKafkaInfo> secondOutPutTag = new OutputTag<TagKafkaInfo>("side-output-second") {
        };
        SingleOutputStreamOperator<TagKafkaInfo> tagKafkaInfoDataStreamSource = env
                // 2.1 添加数据源
                .addSource(MyKafkaUtilDev.getKafkaPojoConsumerWM(
                        ConfigManager.getProperty("kafka.dwd.topic"),
                        "test_" + System.currentTimeMillis())
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
                        if (value.getTaskName().equals("kdj_macd_model")) {
                            if (sideOutPutTags.containsKey(value.getCurrCal())) {
                                ctx.output(sideOutPutTags.get(value.getCurrCal()), value);
                            }
                        }

                    }
                })
                .name("source2sides");

        // 获取到对应计算的数据
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
        DataStream<TagKafkaInfo> pseqDs = tagKafkaInfoDataStreamSource.getSideOutput(sideOutPutTags.get(PropertiesConstants.PSEQ));
        DataStream<TagKafkaInfo> rangeDs = tagKafkaInfoDataStreamSource.getSideOutput(sideOutPutTags.get(PropertiesConstants.RANGE));
        DataStream<TagKafkaInfo> slopeDs = tagKafkaInfoDataStreamSource.getSideOutput(sideOutPutTags.get(PropertiesConstants.SLOPE));
        DataStream<TagKafkaInfo> stdDs = tagKafkaInfoDataStreamSource.getSideOutput(sideOutPutTags.get(PropertiesConstants.STD));
        DataStream<TagKafkaInfo> varianceDs = tagKafkaInfoDataStreamSource.getSideOutput(sideOutPutTags.get(PropertiesConstants.VARIANCE));
        DataStream<TagKafkaInfo> sumDs = tagKafkaInfoDataStreamSource.getSideOutput(sideOutPutTags.get(PropertiesConstants.SUM));
        DataStream<TagKafkaInfo> rawDs = tagKafkaInfoDataStreamSource.getSideOutput(sideOutPutTags.get(PropertiesConstants.RAW));
        DataStream<TagKafkaInfo> kfDs = tagKafkaInfoDataStreamSource.getSideOutput(sideOutPutTags.get(PropertiesConstants.KF));

        SingleOutputStreamOperator<TagKafkaInfo> resultAVGDS = avgDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of())
                .process(new AvgProcessFunc(dwdOutPutTag))
                .name("AVG");

        SingleOutputStreamOperator<TagKafkaInfo> resultLASTDS = lastDs.keyBy(r -> r.getBytName())
                .process(new LastProcessFunc(dwdOutPutTag))
                .name("LAST");

        SingleOutputStreamOperator<TagKafkaInfo> resultMAXDS = maxDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of())
                .process(new MaxProcessFunc(dwdOutPutTag))
                .name("MAX");

        SingleOutputStreamOperator<TagKafkaInfo> resultMINDS = minDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of())
                .process(new MinProcessFunc(dwdOutPutTag))
                .name("MIN");

        SingleOutputStreamOperator<TagKafkaInfo> resultMEDIANDS = medianDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of())
                .process(new MedianProcessFunc(dwdOutPutTag))
                .name("MEDIAN");

        SingleOutputStreamOperator<TagKafkaInfo> resultCVDS = cvDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of())
                .process(new CvProcessFunc(dwdOutPutTag))
                .name("CV");

        SingleOutputStreamOperator<TagKafkaInfo> resultDEJUMPDS = dejumpDs.keyBy(r -> r.getBytName())
                .process(new DejumpProcessFunc(dwdOutPutTag))
                .name("DEJUMP");

        SingleOutputStreamOperator<TagKafkaInfo> resultFOFDS = fofDs.keyBy(r -> r.getBytName())
                .process(new FofProcessFunc(dwdOutPutTag))
                .name("FOF");

        SingleOutputStreamOperator<TagKafkaInfo> resultINTERPDS = interpDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of())
                .process(new InterpProcessFunc(dwdOutPutTag))
                .name("INTERP");

        SingleOutputStreamOperator<TagKafkaInfo> resultTRENDDS = trendDs.keyBy(r -> r.getBytName())
                .process(new TrendProcessFunc(dwdOutPutTag));

        SingleOutputStreamOperator<TagKafkaInfo> resultVARDS = trendDs.keyBy(r -> r.getBytName())
                .process(new VarProcessFunc(dwdOutPutTag))
                .name("VAR");

        SingleOutputStreamOperator<TagKafkaInfo> resultPSEQDS = pseqDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of())
                .process(new PseqProcessFunc(dwdOutPutTag))
                .name("PSEQ");

        SingleOutputStreamOperator<TagKafkaInfo> resultRANGEDS = rangeDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of())
                .process(new RangeProcessFunc(dwdOutPutTag))
                .name("RANGE");

        SingleOutputStreamOperator<TagKafkaInfo> resultSLOPEDS = slopeDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of())
                .process(new SlopeProcessFunc(dwdOutPutTag))
                .name("SLOPE");

        SingleOutputStreamOperator<TagKafkaInfo> resultSTDDS = stdDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of())
                .process(new StdProcessFunc(dwdOutPutTag))
                .name("STD");

        SingleOutputStreamOperator<TagKafkaInfo> resultVARIANCEDS = varianceDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of())
                .process(new VarianceProcessFunc(dwdOutPutTag))
                .name("VARIANCE");

        SingleOutputStreamOperator<TagKafkaInfo> resultSUMDS = sumDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of())
                .sum("value")
                .name("SUM");

        SingleOutputStreamOperator<TagKafkaInfo> resultKFDS = kfDs.keyBy(r -> r.getBytName())
                .process(new KfProcessFunc(dwdOutPutTag))
                .name("KF");


        // 获取还需进一步计算的数据
        DataStream<TagKafkaInfo> sideOutputAVG = resultAVGDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputMAX = resultMAXDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputMIN = resultMINDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputMEDIAN = resultMEDIANDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputLAST = resultLASTDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputCV = resultCVDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputDEJUMP = resultDEJUMPDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputINTERP = resultINTERPDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputFOF = resultFOFDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputTREND = resultTRENDDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputVAR = resultVARDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputPSEQ = resultPSEQDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputRANGE = resultRANGEDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputSLOPE = resultSLOPEDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputSTD = resultSTDDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputVARIANCE = resultVARIANCEDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputSUM = resultSUMDS.getSideOutput(dwdOutPutTag);
        DataStream<TagKafkaInfo> sideOutputKF = resultKFDS.getSideOutput(dwdOutPutTag);

        // union后续计算数据
        SingleOutputStreamOperator<String> dwdResult = sideOutputAVG
                .union(
                        sideOutputMAX, sideOutputMIN, sideOutputLAST,
                        sideOutputMEDIAN, sideOutputCV, sideOutputDEJUMP,
                        sideOutputFOF, sideOutputINTERP, sideOutputTREND,
                        sideOutputVAR, sideOutputPSEQ, sideOutputRANGE,
                        sideOutputSLOPE, sideOutputSTD, sideOutputVARIANCE,
                        sideOutputSUM, sideOutputKF
                )
                .map(new MapPojo2JsonStr<TagKafkaInfo>())
                .name("dwd-union");
        dwdResult.print("dwd>>>");
        dwdResult.addSink(MyKafkaUtilDev.getKafkaProducer(ConfigManager.getProperty(PropertiesConstants.KAFKA_DWD_TOPIC)))
                .name("dwd-sink");

        // union计算完成数据
        DataStream<TagKafkaInfo> dwsResult = resultAVGDS
                .union(
                        resultMAXDS, resultMINDS, resultLASTDS,
                        resultMEDIANDS, resultCVDS, sideOutputDEJUMP,
                        resultFOFDS, resultINTERPDS, resultTRENDDS,
                        resultVARDS, resultPSEQDS, resultRANGEDS,
                        resultSLOPEDS, resultSTDDS, resultVARIANCEDS,
                        resultSUMDS, rawDs, sideOutputKF
                );
        // send to kafka
        dwsResult
                .map(new MapPojo2JsonStr<TagKafkaInfo>())
                .addSink(MyKafkaUtilDev.getKafkaProducer(ConfigManager.getProperty(PropertiesConstants.KAFKA_DWS_TOPIC)))
                .name("dws-sink");

        // 划分分钟级别数据和秒级别数据
        SingleOutputStreamOperator<TagKafkaInfo> minuteResult = dwsResult
                .process(new ProcessFunction<TagKafkaInfo, TagKafkaInfo>() {
                    @Override
                    public void processElement(TagKafkaInfo value, ProcessFunction<TagKafkaInfo, TagKafkaInfo>.Context ctx, Collector<TagKafkaInfo> out) throws Exception {
                        if (value.getWinSlide() != null && value.getWinSlide().contains("s")) {
                            ctx.output(secondOutPutTag, value);
                        } else {
                            out.collect(value);
                        }
                    }
                });
        DataStream<TagKafkaInfo> secondResult = minuteResult.getSideOutput(secondOutPutTag);

        // send to mysql 分钟级别数据
//        minuteResult
//                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1L)))
//                .process(new BatchOutAllWindowFunction())
//                .addSink(new DbResultBatchSink(ConfigManager.getProperty(PropertiesConstants.DWS_TODAY_TABLE)))
//                .name("dws_tag_minute");


        // send to mysql 秒级别级别数据
        secondResult
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1L)))
                .process(new BatchOutAllWindowFunction())
                .addSink(new DbResultBatchSink(ConfigManager.getProperty(PropertiesConstants.DWS_SECOND_TABLE)))
                .name("dws_tag_second");

        // TODO 8.启动任务
        env.execute("dws_arithmetic_job");
    }

}
