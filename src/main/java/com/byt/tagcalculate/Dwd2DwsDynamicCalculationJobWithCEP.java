package com.byt.tagcalculate;

import com.byt.common.utils.ConfigManager;
import com.byt.common.utils.MyKafkaUtil;
import com.byt.common.utils.SideOutPutTagUtil;
import com.byt.tagcalculate.calculate.func.*;
import com.byt.tagcalculate.constants.PropertiesConstants;
import com.byt.tagcalculate.func.BatchOutAllWindowFunction;
import com.byt.tagcalculate.func.MapPojo2JsonStr;
import com.byt.tagcalculate.func.PreOrSecondResultFunction;
import com.byt.tagcalculate.func.PreProcessFunction;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.tagcalculate.sink.DbResultBatchSink;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.listern.CepListener;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.DynamicSlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TimeAdjustExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.HashMap;
import java.util.Set;

/**
 * @title: DWS动态计算
 * @author: zhangyifan
 * @date: 2023/7/10 16:51
 */
public class Dwd2DwsDynamicCalculationJobWithCEP {

    public static void main(String[] args) throws Exception {
        // TODO 0.获取执行环境和相关参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 定义测输出流标签
        HashMap<String, OutputTag<TagKafkaInfo>> sideOutPutTags = SideOutPutTagUtil.getSideOutPutTags();
        Set<String> strings = sideOutPutTags.keySet();
        for (String string : strings) {
            System.out.println(string + "------------------>" + sideOutPutTags.get(string));
        }

        OutputTag<TagKafkaInfo> dwdOutPutTag = new OutputTag<TagKafkaInfo>("side-output-dwd") {
        };
        OutputTag<TagKafkaInfo> secondOutPutTag = new OutputTag<TagKafkaInfo>("side-output-second") {
        };
        OutputTag<TagKafkaInfo> preOutPutTag = new OutputTag<TagKafkaInfo>("side-output-pre") {
        }; // 中间算子回流通道

        SingleOutputStreamOperator<TagKafkaInfo> tagKafkaInfoDataStreamSource = env
                // 2.1 添加数据源
                .addSource(MyKafkaUtil.getKafkaPojoConsumerWM(
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
                        if (sideOutPutTags.containsKey(value.getCurrCal())) {
                            ctx.output(sideOutPutTags.get(value.getCurrCal()), value);
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
                .window(DynamicSlidingEventTimeWindows.<TagKafkaInfo>of(
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSize();
                            }
                        },
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSlide();
                            }
                        }
                ))
                .process(new AvgProcessFunc(dwdOutPutTag))
                .name("AVG");

        SingleOutputStreamOperator<TagKafkaInfo> resultMAXDS = maxDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of(
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSize();
                            }
                        },
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSlide();
                            }
                        }
                ))
                .process(new MaxProcessFunc(dwdOutPutTag))
                .name("MAX");

        SingleOutputStreamOperator<TagKafkaInfo> resultMINDS = minDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of(
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSize();
                            }
                        },
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSlide();
                            }
                        }
                ))
                .process(new MinProcessFunc(dwdOutPutTag))
                .name("MIN");

        SingleOutputStreamOperator<TagKafkaInfo> resultMEDIANDS = medianDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of(
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSize();
                            }
                        },
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSlide();
                            }
                        }
                ))
                .process(new MedianProcessFunc(dwdOutPutTag))
                .name("MEDIAN");

        SingleOutputStreamOperator<TagKafkaInfo> resultCVDS = cvDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of(
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSize();
                            }
                        },
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSlide();
                            }
                        }
                ))
                .process(new CvProcessFunc(dwdOutPutTag))
                .name("CV");

        SingleOutputStreamOperator<TagKafkaInfo> resultINTERPDS = interpDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of(
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSize();
                            }
                        },
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSlide();
                            }
                        }
                ))
                .process(new InterpProcessFunc(dwdOutPutTag))
                .name("INTERP");

        SingleOutputStreamOperator<TagKafkaInfo> resultPSEQDS = pseqDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of(
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSize();
                            }
                        },
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSlide();
                            }
                        }
                ))
                .process(new PseqProcessFunc(dwdOutPutTag))
                .name("PSEQ");

        SingleOutputStreamOperator<TagKafkaInfo> resultRANGEDS = rangeDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of(
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSize();
                            }
                        },
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSlide();
                            }
                        }
                ))
                .process(new RangeProcessFunc(dwdOutPutTag))
                .name("RANGE");

        SingleOutputStreamOperator<TagKafkaInfo> resultSLOPEDS = slopeDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of(
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSize();
                            }
                        },
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSlide();
                            }
                        }
                ))
                .process(new SlopeProcessFunc(dwdOutPutTag))
                .name("SLOPE");

        SingleOutputStreamOperator<TagKafkaInfo> resultSTDDS = stdDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of(
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSize();
                            }
                        },
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSlide();
                            }
                        }
                ))
                .process(new StdProcessFunc(dwdOutPutTag))
                .name("STD");

        SingleOutputStreamOperator<TagKafkaInfo> resultVARIANCEDS = varianceDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of(
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSize();
                            }
                        },
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSlide();
                            }
                        }
                ))
                .process(new VarianceProcessFunc(dwdOutPutTag))
                .name("VARIANCE");

        SingleOutputStreamOperator<TagKafkaInfo> resultSUMDS = sumDs.keyBy(r -> r.getBytName())
                .window(DynamicSlidingEventTimeWindows.of(
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSize();
                            }
                        },
                        new TimeAdjustExtractor<TagKafkaInfo>() {
                            @Override
                            public long extract(TagKafkaInfo element) {
                                return element.getWinSlide();
                            }
                        }
                ))
                .process(new SumProcessFunc(dwdOutPutTag))
                .name("SUM");

        SingleOutputStreamOperator<TagKafkaInfo> resultDEJUMPDS = dejumpDs.keyBy(r -> r.getBytName())
                .process(new DejumpProcessFunc(dwdOutPutTag))
                .name("DEJUMP");

        SingleOutputStreamOperator<TagKafkaInfo> resultFOFDS = fofDs.keyBy(r -> r.getBytName())
                .process(new FofProcessFunc(dwdOutPutTag))
                .name("FOF");


        // CEP PATTERN FOR LAST、VAR、TREND
        // 定义匹配规则
        Pattern<TagKafkaInfo, TagKafkaInfo> pattern = Pattern.<TagKafkaInfo>begin("tag")
                .times(2).consecutive();
        System.out.println("init:---" + pattern);


        SingleOutputStreamOperator<TagKafkaInfo> cepLASTDS = lastDs
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TagKafkaInfo>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                                .withTimestampAssigner(new SerializableTimestampAssigner<TagKafkaInfo>() {
                                    @Override
                                    public long extractTimestamp(TagKafkaInfo tagKafkaInfo, long l) {
                                        return tagKafkaInfo.getTimestamp();
                                    }
                                })

                )
                .keyBy(r -> r.getBytName())
                .process(new CepProcessFunc());

        cepLASTDS.print("cep:");

        SingleOutputStreamOperator<TagKafkaInfo> resultLASTDS = CEP
                .pattern(cepLASTDS.keyBy(r -> r.getBytName()), pattern)
                .registerListener(new CepListener<TagKafkaInfo>() {
                    @Override
                    public Boolean needChange(TagKafkaInfo tagKafkaInfo) {
                        return tagKafkaInfo.getChangeNFA();
                    }

                    @Override
                    public Pattern<TagKafkaInfo, ?> returnPattern(TagKafkaInfo tagKafkaInfo) {
                        Integer n = tagKafkaInfo.getCurrNBefore();
                        System.out.println("接收到数据:" + n + "感知到切换逻辑");
                        Pattern<TagKafkaInfo, TagKafkaInfo> p = Pattern.<TagKafkaInfo>begin("tag")
                                .times(n + 1).consecutive();
                        System.out.println("P-----:" + p);
                        return p;
                    }
                })
                .process(new CepPatternProcessFunc(dwdOutPutTag))
                .name("LAST");


        SingleOutputStreamOperator<TagKafkaInfo> cepTRENDDS = trendDs
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TagKafkaInfo>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                                .withTimestampAssigner(new SerializableTimestampAssigner<TagKafkaInfo>() {
                                    @Override
                                    public long extractTimestamp(TagKafkaInfo tagKafkaInfo, long l) {
                                        return tagKafkaInfo.getTimestamp();
                                    }
                                })

                )
                .keyBy(r -> r.getBytName())
                .process(new CepProcessFunc());
        SingleOutputStreamOperator<TagKafkaInfo> resultTRENDDS = CEP
                .pattern(cepTRENDDS.keyBy(r -> r.getBytName()), pattern)
                .registerListener(new CepListener<TagKafkaInfo>() {
                    @Override
                    public Boolean needChange(TagKafkaInfo tagKafkaInfo) {
                        return tagKafkaInfo.getChangeNFA();
                    }

                    @Override
                    public Pattern<TagKafkaInfo, ?> returnPattern(TagKafkaInfo tagKafkaInfo) {
                        Integer n = tagKafkaInfo.getCurrNBefore();
                        System.out.println("接收到数据:" + n + "感知到切换逻辑");
                        return Pattern.<TagKafkaInfo>begin("tag")
                                .times(n + 1)
                                .consecutive();
                    }
                })
                .process(new CepPatternProcessFunc(dwdOutPutTag))
                .name("TREND");



        SingleOutputStreamOperator<TagKafkaInfo> cepVARDS = varDs
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TagKafkaInfo>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                                .withTimestampAssigner(new SerializableTimestampAssigner<TagKafkaInfo>() {
                                    @Override
                                    public long extractTimestamp(TagKafkaInfo tagKafkaInfo, long l) {
                                        return tagKafkaInfo.getTimestamp();
                                    }
                                })

                )
                .keyBy(r -> r.getBytName())
                .process(new CepProcessFunc());

        SingleOutputStreamOperator<TagKafkaInfo> resultVARDS = CEP
                .pattern(cepVARDS.keyBy(r -> r.getBytName()), pattern)
                .registerListener(new CepListener<TagKafkaInfo>() {
                    @Override
                    public Boolean needChange(TagKafkaInfo tagKafkaInfo) {
                        return tagKafkaInfo.getChangeNFA();
                    }

                    @Override
                    public Pattern<TagKafkaInfo, ?> returnPattern(TagKafkaInfo tagKafkaInfo) {
                        Integer n = tagKafkaInfo.getCurrNBefore();
                        System.out.println("接收到数据:" + n + "感知到切换逻辑");
                        return Pattern.<TagKafkaInfo>begin("tag")
                                .times(n + 1)
                                .consecutive();
                    }
                })
                .process(new CepPatternProcessFunc(dwdOutPutTag))
                .name("VAR");


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
                        sideOutputFOF, sideOutputINTERP,
                        sideOutputTREND,
                        sideOutputVAR,
                        sideOutputPSEQ, sideOutputRANGE,
                        sideOutputSLOPE, sideOutputSTD, sideOutputVARIANCE,
                        sideOutputSUM, sideOutputKF
                )
                .map(new MapPojo2JsonStr<TagKafkaInfo>())
                .name("dwd-union");
        //dwdResult.print("dwd>>>");
        dwdResult.addSink(MyKafkaUtil.getKafkaProducer(ConfigManager.getProperty(PropertiesConstants.KAFKA_DWD_TOPIC)))
                .name("dwd-sink");

        // union计算完成数据
        DataStream<TagKafkaInfo> dwsResult = resultAVGDS
                .union(
                        resultMAXDS, resultMINDS, resultLASTDS,
                        resultMEDIANDS, resultCVDS, resultDEJUMPDS,
                        resultFOFDS, resultINTERPDS,
                        resultTRENDDS, resultVARDS,
                        resultPSEQDS, resultRANGEDS,
                        resultSLOPEDS, resultSTDDS, resultVARIANCEDS,
                        resultSUMDS, rawDs, resultKFDS
                );
        // send to kafka
        dwsResult
                .map(new MapPojo2JsonStr<TagKafkaInfo>())
                .addSink(MyKafkaUtil.getKafkaProducer(ConfigManager.getProperty(PropertiesConstants.KAFKA_DWS_TOPIC)))
                .name("dws-sink");

        dwsResult.print("dws<<<");

        // 划分分钟级别数据、秒级别数据和中间算子数据
        SingleOutputStreamOperator<TagKafkaInfo> minuteResult = dwsResult
                .process(new PreOrSecondResultFunction(preOutPutTag, secondOutPutTag));

        DataStream<TagKafkaInfo> secondResult = minuteResult.getSideOutput(secondOutPutTag); // 秒级别数据
        DataStream<TagKafkaInfo> preResult = minuteResult.getSideOutput(preOutPutTag); // 中间算子回流数据


        // send to kafka(tag_pre)
        preResult
                .keyBy(r -> r.getTime())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1L)))
                .process(new PreProcessFunction())
                .addSink(MyKafkaUtil.getProducerWithTopicData())
                .name("中间算子回流");

        // send to mysql 分钟级别数据
        minuteResult
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1L)))
                .process(new BatchOutAllWindowFunction())
                .addSink(new DbResultBatchSink(ConfigManager.getProperty(PropertiesConstants.DWS_TODAY_TABLE)))
                .name("dws_tag_minute");


        // send to mysql 秒级别级别数据
        secondResult
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1L)))
                .process(new BatchOutAllWindowFunction())
                .addSink(new DbResultBatchSink(ConfigManager.getProperty(PropertiesConstants.DWS_SECOND_TABLE)))
                .name("dws_tag_second");

        // TODO 8.启动任务
        env.execute("dws_dynamicCalculation_job");
    }

}
