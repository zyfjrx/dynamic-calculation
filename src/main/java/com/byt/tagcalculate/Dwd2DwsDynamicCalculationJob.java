package com.byt.tagcalculate;

import com.byt.common.utils.EnvironmentUtils;
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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
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
public class Dwd2DwsDynamicCalculationJob {

    public static void main(String[] args) throws Exception {
        // 获取执行环境和相关参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool parameterTool = EnvironmentUtils.createParameterTool();
        env.getConfig().setGlobalJobParameters(parameterTool);
        // RocksDB状态后端
        //env.setStateBackend(new EmbeddedRocksDBStateBackend());
        if (parameterTool.getBoolean("flink.checkpoint.is-enable")) {
            env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE);
            // 设置检查点超时时间
            env.getCheckpointConfig().setCheckpointTimeout(12 * 60 * 1000L);
            // 设置取消job后，检查点是否保留
            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            // 设置重启策略
            // 固定次数重启
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
            // 失败率重启
            // env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.milliseconds(3000), Time.days(30)));
            // 设置检查点间隔时间
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(6 * 60 * 1000L);
            // 设置状态后段
            env.setStateBackend(new FsStateBackend("hdfs://" + parameterTool.get("hdfs.node") + "/flink/dynamic/dws"));
            // 设置操作hadoop用户
            System.setProperty("HADOOP_USER_NAME", parameterTool.get("hdfs.user"));
        }

        // 定义测输出流标签
        HashMap<String, OutputTag<TagKafkaInfo>> sideOutPutTags = SideOutPutTagUtil.getSideOutPutTags();
        Set<String> strings = sideOutPutTags.keySet();
//        for (String string : strings) {
//            System.out.println(string + "------------------>" + sideOutPutTags.get(string));
//        }

        OutputTag<TagKafkaInfo> dwdOutPutTag = new OutputTag<TagKafkaInfo>("side-output-dwd") {
        };
        OutputTag<TagKafkaInfo> secondOutPutTag = new OutputTag<TagKafkaInfo>("side-output-second") {
        };
        OutputTag<TagKafkaInfo> preOutPutTag = new OutputTag<TagKafkaInfo>("side-output-pre") {
        }; // 中间算子回流通道

        SingleOutputStreamOperator<TagKafkaInfo> kafkaSource = env
                // 2.1 添加数据源
                .addSource(MyKafkaUtil.getKafkaPojoConsumer(
                        parameterTool.get("kafka.dwd.topic"),
                        "test2_20230808",
                        parameterTool.get("kafka.server"))
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TagKafkaInfo>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                                .withIdleness(Duration.ofSeconds(5L))
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


//  =============================================  获取对应算子流数据开始计算  ==============================================

        SingleOutputStreamOperator<TagKafkaInfo> resultAVGDS = kafkaSource
                .getSideOutput(sideOutPutTags.get(PropertiesConstants.AVG))
                .keyBy(r -> r.getBytName())
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
                .aggregate(
                        new AvgAggregateFunction(),
                        new ResultProcessWindowFunction(dwdOutPutTag)
                )
                .name("AVG");

        SingleOutputStreamOperator<TagKafkaInfo> resultMAXDS = kafkaSource
                .getSideOutput(sideOutPutTags.get(PropertiesConstants.MAX))
                .keyBy(r -> r.getBytName())
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
                .aggregate(
                        new MaxAggregateFunction(),
                        new ResultProcessWindowFunction(dwdOutPutTag)
                )
                .name("MAX");

        SingleOutputStreamOperator<TagKafkaInfo> resultMINDS = kafkaSource
                .getSideOutput(sideOutPutTags.get(PropertiesConstants.MIN))
                .keyBy(r -> r.getBytName())
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
                .aggregate(
                        new MinAggregateFunction(),
                        new ResultProcessWindowFunction(dwdOutPutTag)
                )
                .name("MIN");

        SingleOutputStreamOperator<TagKafkaInfo> resultMEDIANDS = kafkaSource
                .getSideOutput(sideOutPutTags.get(PropertiesConstants.MEDIAN))
                .keyBy(r -> r.getBytName())
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

        SingleOutputStreamOperator<TagKafkaInfo> resultCVDS = kafkaSource
                .getSideOutput(sideOutPutTags.get(PropertiesConstants.CV))
                .keyBy(r -> r.getBytName())
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

        SingleOutputStreamOperator<TagKafkaInfo> resultINTERPDS = kafkaSource
                .getSideOutput(sideOutPutTags.get(PropertiesConstants.INTERP))
                .keyBy(r -> r.getBytName())
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
                .aggregate(
                        new InterpAggregateFunction(),
                        new ResultProcessWindowFunction(dwdOutPutTag)
                )
                .name("INTERP");

        SingleOutputStreamOperator<TagKafkaInfo> resultPSEQDS = kafkaSource
                .getSideOutput(sideOutPutTags.get(PropertiesConstants.PSEQ))
                .keyBy(r -> r.getBytName())
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
                .aggregate(
                        new PseqAggregateFunction(),
                        new ResultProcessWindowFunction(dwdOutPutTag)
                )
                .name("PSEQ");

        SingleOutputStreamOperator<TagKafkaInfo> resultRANGEDS = kafkaSource
                .getSideOutput(sideOutPutTags.get(PropertiesConstants.RANGE))
                .keyBy(r -> r.getBytName())
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
                .aggregate(
                        new RangeAggregateFunction(),
                        new ResultProcessWindowFunction(dwdOutPutTag)
                )
                .name("RANGE");

        SingleOutputStreamOperator<TagKafkaInfo> resultSLOPEDS = kafkaSource
                .getSideOutput(sideOutPutTags.get(PropertiesConstants.SLOPE))
                .keyBy(r -> r.getBytName())
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

        SingleOutputStreamOperator<TagKafkaInfo> resultSTDDS = kafkaSource
                .getSideOutput(sideOutPutTags.get(PropertiesConstants.STD))
                .keyBy(r -> r.getBytName())
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

        SingleOutputStreamOperator<TagKafkaInfo> resultVARIANCEDS = kafkaSource
                .getSideOutput(sideOutPutTags.get(PropertiesConstants.VARIANCE))
                .keyBy(r -> r.getBytName())
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

        SingleOutputStreamOperator<TagKafkaInfo> resultSUMDS = kafkaSource
                .getSideOutput(sideOutPutTags.get(PropertiesConstants.SUM))
                .keyBy(r -> r.getBytName())
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
                .aggregate(
                        new SumAggregateFunction(),
                        new ResultProcessWindowFunction(dwdOutPutTag)
                )
                //.process(new SumProcessFunc(dwdOutPutTag))
                .name("SUM");

        SingleOutputStreamOperator<TagKafkaInfo> resultDEJUMPDS = kafkaSource
                .getSideOutput(sideOutPutTags.get(PropertiesConstants.DEJUMP))
                .keyBy(r -> r.getBytName())
                .process(new DejumpProcessFunc(dwdOutPutTag))
                .name("DEJUMP");

        SingleOutputStreamOperator<TagKafkaInfo> resultFOFDS = kafkaSource
                .getSideOutput(sideOutPutTags.get(PropertiesConstants.FOF))
                .keyBy(r -> r.getBytName())
                .process(new FofProcessFunc(dwdOutPutTag))
                .name("FOF");

        SingleOutputStreamOperator<TagKafkaInfo> resultLASTDS = kafkaSource
                .getSideOutput(sideOutPutTags.get(PropertiesConstants.LAST))
                .keyBy(r -> r.getBytName())
                .process(new LastProcessFunc(dwdOutPutTag))
                .name("LAST");

        SingleOutputStreamOperator<TagKafkaInfo> resultTRENDDS = kafkaSource
                .getSideOutput(sideOutPutTags.get(PropertiesConstants.TREND))
                .keyBy(r -> r.getBytName())
                .process(new TrendProcessFunc(dwdOutPutTag))
                .name("TREND");


        SingleOutputStreamOperator<TagKafkaInfo> resultVARDS = kafkaSource
                .getSideOutput(sideOutPutTags.get(PropertiesConstants.VAR))
                .keyBy(r -> r.getBytName())
                .process(new VarProcessFunc(dwdOutPutTag))
                .name("VAR");


        SingleOutputStreamOperator<TagKafkaInfo> resultKFDS = kafkaSource
                .getSideOutput(sideOutPutTags.get(PropertiesConstants.KF))
                .keyBy(r -> r.getBytName())
                .process(new KfProcessFunc(dwdOutPutTag))
                .name("KF");

        SingleOutputStreamOperator<TagKafkaInfo> resultEMADS = kafkaSource
                .getSideOutput(sideOutPutTags.get(PropertiesConstants.EMA))
                .keyBy(r -> r.getBytName())
                .process(new EmaProcessFunc(dwdOutPutTag))
                .name("EMA");

        SingleOutputStreamOperator<TagKafkaInfo> resultRSIDS = kafkaSource
                .getSideOutput(sideOutPutTags.get(PropertiesConstants.RSI))
                .keyBy(r -> r.getBytName())
                .process(new RsiProcessFunc(dwdOutPutTag))
                .name("RSI");


//  ===============================================  获取对应算子计算完毕   ================================================


// ====================================================== DWD =========================================================
        // 获取还需进一步计算的数据,union后续计算数据
        SingleOutputStreamOperator<String> dwdResult = resultAVGDS
                .getSideOutput(dwdOutPutTag)
                .union(
                        resultMAXDS.getSideOutput(dwdOutPutTag),
                        resultMINDS.getSideOutput(dwdOutPutTag),
                        resultLASTDS.getSideOutput(dwdOutPutTag),
                        resultMEDIANDS.getSideOutput(dwdOutPutTag),
                        resultCVDS.getSideOutput(dwdOutPutTag),
                        resultDEJUMPDS.getSideOutput(dwdOutPutTag),
                        resultFOFDS.getSideOutput(dwdOutPutTag),
                        resultINTERPDS.getSideOutput(dwdOutPutTag),
                        resultTRENDDS.getSideOutput(dwdOutPutTag),
                        resultVARDS.getSideOutput(dwdOutPutTag),
                        resultPSEQDS.getSideOutput(dwdOutPutTag),
                        resultRANGEDS.getSideOutput(dwdOutPutTag),
                        resultSLOPEDS.getSideOutput(dwdOutPutTag),
                        resultSTDDS.getSideOutput(dwdOutPutTag),
                        resultVARIANCEDS.getSideOutput(dwdOutPutTag),
                        resultSUMDS.getSideOutput(dwdOutPutTag),
                        resultKFDS.getSideOutput(dwdOutPutTag),
                        resultEMADS.getSideOutput(dwdOutPutTag),
                        resultRSIDS.getSideOutput(dwdOutPutTag)
                )
                .map(new MapPojo2JsonStr<TagKafkaInfo>())
                .name("dwd-union");
        //dwdResult.print("dwd>>>");
        dwdResult.addSink(MyKafkaUtil.getKafkaProducer(parameterTool.get("kafka.dwd.topic"),parameterTool.get("kafka.server")))
                .name("dwd-sink");
// ======================================================= DWD =========================================================


// ======================================================= DWS =========================================================
        // union计算完成数据
        DataStream<TagKafkaInfo> dwsResult = resultAVGDS
                .union(
                        resultMAXDS, resultMINDS, resultLASTDS,
                        resultMEDIANDS, resultCVDS, resultDEJUMPDS,
                        resultFOFDS, resultINTERPDS, resultTRENDDS,
                        resultVARDS, resultPSEQDS, resultRANGEDS,
                        resultSLOPEDS, resultSTDDS, resultVARIANCEDS,
                        resultSUMDS, resultKFDS,
                        kafkaSource.getSideOutput(sideOutPutTags.get(PropertiesConstants.RAW)),
                        resultEMADS, resultRSIDS
                );
        // send to kafka
        dwsResult
                .map(new MapPojo2JsonStr<TagKafkaInfo>())
                .addSink(MyKafkaUtil.getKafkaProducer(parameterTool.get("kafka.dws.topic"),parameterTool.get("kafka.server")))
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
                .addSink(new DbResultBatchSink(parameterTool.get("dws.today.table")))
                .name("dws_tag_minute_today");

        minuteResult
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1L)))
                .process(new BatchOutAllWindowFunction())
                .addSink(new DbResultBatchSink(parameterTool.get("dws.result.table")))
                .name("dws_tag_minute_result");


        // send to mysql 秒级别级别数据
        secondResult
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1L)))
                .process(new BatchOutAllWindowFunction())
                .addSink(new DbResultBatchSink(parameterTool.get("dws.second.table")))
                .name("dws_tag_second");

// ======================================================= DWS =========================================================

        env.execute("dws_dynamicCalculation_job");
    }

}
