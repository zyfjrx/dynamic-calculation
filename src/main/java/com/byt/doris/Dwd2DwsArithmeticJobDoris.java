package com.byt.doris;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.byt.constants.PropertiesConstants;
import com.byt.func.*;
import com.byt.pojo.TagKafkaInfo;
import com.byt.sink.DbResultBatchSink;
import com.byt.utils.*;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.*;

/**
 * @title: DWS动态计算
 * @author: zhangyifan
 * @date: 2022/8/29 16:51
 */
public class Dwd2DwsArithmeticJobDoris {

    public static void main(String[] args) throws Exception {
        // TODO 0.获取执行环境和相关参数
        StreamExecutionEnvironment env = StreamEnvUtil.getEnv("dwd2dws");
        env.setParallelism(1);
        // 产线id
        String lineId = args[0];
        // 产线下对应的上线任务列表
        String jobNames = args[1];
        String jobName = JobUtils.geJobName(jobNames);

        // TODO 1.通过配置创建不同类型的侧输出流对象
        Set<String> sideParams = new HashSet<>();
        sideParams = SideOutPutTagUtil.getSideParamsWithJobs(jobNames);
        int times = 3;
        while (sideParams.size() == 0) {
            System.out.println("计算集合为空重新获取中！！！！！");
            sideParams = SideOutPutTagUtil.getSideParamsWithJobs(jobNames);
            times--;
            Thread.sleep(3000L);
            if (times == 0) {
                System.out.println("errorMsg:计算集合为空停止计算！！！");
                System.exit(1);
            }
        }
        System.out.println("sideParams:" + sideParams);
        HashMap<String, OutputTag<TagKafkaInfo>> sideOutPutTags = SideOutPutTagUtil.getSideOutPutTags(sideParams);
        OutputTag<TagKafkaInfo> errorSide = new OutputTag<TagKafkaInfo>("error-side-out") {
        };
        OutputTag<TagKafkaInfo> preOutPutTag = new OutputTag<TagKafkaInfo>("side-output-pre") {
        };
        OutputTag<TagKafkaInfo> secondOutPutTag = new OutputTag<TagKafkaInfo>("second-output") {
        };

        System.out.println(sideOutPutTags);
        // TODO 2.对数据进行ETL、按照计算类型分流（侧输出流）
        SingleOutputStreamOperator<TagKafkaInfo> tagKafkaInfoDataStreamSource = env
                // 2.1 添加数据源
                .addSource(MyKafkaUtilDev.getKafkaPojoConsumerWM(
                        ConfigManager.getProperty(PropertiesConstants.KAFKA_DWD_TOPIC_PREFIX) + lineId,
                        "dwd2dws_" + jobName + "_" + System.currentTimeMillis())
                );
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy.<TagKafkaInfo>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
//                                .withTimestampAssigner(new SerializableTimestampAssigner<TagKafkaInfo>() {
//                                    @Override
//                                    public long extractTimestamp(TagKafkaInfo tagKafkaInfo, long l) {
//                                        return tagKafkaInfo.getTimestamp();
//                                    }
//                                })
//
//                )
        //.setParallelism(2);
        SingleOutputStreamOperator<TagKafkaInfo> mainStream = tagKafkaInfoDataStreamSource
                // 2.2 过滤数据
                .filter(new FilterNormalAndJobName(jobNames))
                // 2.3 按照计算类型分流
                .process(new SideOutPutFunction(sideOutPutTags, errorSide));

        mainStream.print("读取到数据：");

        // mainStream.print();

        // TODO 3.处理计算参数，生成StreamGraph、反射调用相关的计算算子、Union所有类型的计算结果
        List<DataStream<TagKafkaInfo>> streamArray = CoreCalculateFunction
                .coreCalculate(sideParams, mainStream, sideOutPutTags);
        // 将不同类型计算结果Union在一起
        DataStream<TagKafkaInfo> outPutStream = CoreCalculateFunction.unionStream(streamArray);

        outPutStream.print("outPutStream");

        // TODO 4.对预处理和秒级别结果进行分流（sideOutputStream）
        SingleOutputStreamOperator<TagKafkaInfo> mainOutPutStream = outPutStream
                .process(new PreOrSecondResultFunction(preOutPutTag, secondOutPutTag));

        DataStream<TagKafkaInfo> secondSideOutput = mainOutPutStream.getSideOutput(secondOutPutTag);
        DataStream<TagKafkaInfo> preSideOutput = mainOutPutStream.getSideOutput(preOutPutTag);
        // TODO 5.获取预处理流数据,sink到预处理主题
        preSideOutput
                .keyBy(r -> r.getTime())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1L)))
                .process(new PreProcessFunction())
                .addSink(MyKafkaUtilDev.getProducerWithTopicData())
                .name("中间算子回流");
        preSideOutput.map(new MapPojo2JsonStr<TagKafkaInfo>()).print("pre>>>>>");

        // TODO 6.1 秒级别数据异步写入mysql
        SingleOutputStreamOperator<List<TagKafkaInfo>> secondData = secondSideOutput
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1L)))
                .process(new BatchOutAllWindowFunction())
                .name("秒级别数据发送窗口");
        //secondData.print("second:");

        Properties pro = new Properties();
        pro.setProperty("format", "json");
        pro.setProperty("strip_outer_array", "true");
        secondData
                .flatMap(new FlatMapFunction<List<TagKafkaInfo>, String>() {
                    @Override
                    public void flatMap(List<TagKafkaInfo> tagKafkaInfos, Collector<String> collector) throws Exception {
                        for (TagKafkaInfo tagKafkaInfo : tagKafkaInfos) {
                            collector.collect(JSON.toJSONString(tagKafkaInfo));
                        }
                    }
                })
                .addSink(DorisSink.sink(
                        DorisExecutionOptions.builder()
                                .setBatchSize(10000)
                                .setBatchIntervalMs(10L)
                                .setMaxRetries(3)
                                .setStreamLoadProp(pro)
                                .setEnableDelete(true)
                                .build(),
                        DorisOptions.builder()
                                .setFenodes("hadoop202:8030")
                                .setTableIdentifier("test_db.dws_tag_second")
                                .setUsername("root")
                                .setPassword("")
                                .build()
                ))
                .name("dws_tag_second");


        // TODO 6.2 分钟数据异步写入mysql/gp
        SingleOutputStreamOperator<List<TagKafkaInfo>> batchOutPut = mainOutPutStream
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1L)))
                .process(new BatchOutAllWindowFunction())
                .name("分钟级别数据发送窗口");
        //batchOutPut.print("default:");
        batchOutPut
                .addSink(new DbResultBatchSink(ConfigManager.getProperty(PropertiesConstants.DWS_RESULT_TABLE)))
                .name("dws_tag_result>:");
        batchOutPut
                .addSink(new DbResultBatchSink(ConfigManager.getProperty(PropertiesConstants.DWS_TODAY_TABLE)))
                .name("dws_tag_today>:");

        // TODO 7.计算结果实时写入kafka
        SingleOutputStreamOperator<String> resultDS = mainOutPutStream
                .union(secondSideOutput)
                .map(new MapPojo2JsonStr<TagKafkaInfo>());
        //TODO dev
        resultDS.addSink(MyKafkaUtilDev.getKafkaProducer(ConfigManager.getProperty(PropertiesConstants.KAFKA_DWS_TOPIC)));
        /*resultDS.addSink(
                        MyKafkaUtilDev.getKafkaSinkBySchema(new KafkaSerializationSchema<String>() {
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long aLong) {
                                JSONObject jsonObject = JSON.parseObject(s);
                                String lineId = jsonObject.getString("lineId");
                                String bytName = jsonObject.getString("bytName");
                                String topic = "dws_line_" + lineId;
                                //System.out.println(sink_topic + "--" + s);
                                return new ProducerRecord<byte[], byte[]>(topic,bytName.getBytes(StandardCharsets.UTF_8), s.getBytes(StandardCharsets.UTF_8));
                            }
                        })
                )
                .setParallelism(ConfigManager.getInteger(PropertiesConstants.STREAM_SINK_DWS_PARALLELISM))
                .name("sink to kafka");*/
        resultDS.print("dws:>");

        // TODO 8.启动任务
        env.execute("dws_arithmetic_line<" + lineId + ">_" + jobName);
    }
}
// crontab 定时任务
// 0 */6 * * * bash /opt/jupiterApp/flink-project/delSecondData.sh
// 00 6 * * * bash /opt/jupiterApp/flink-project/delTodayData.sh
