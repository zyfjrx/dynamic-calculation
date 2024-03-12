package com.byt.tagcalculate;
import com.byt.common.utils.DorisUtil;
import com.byt.common.utils.EnvironmentUtils;
import com.byt.common.utils.MyKafkaUtil;
import com.byt.tagcalculate.func.BeanToJsonStrFunction;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @title:
 * @author: zhangyf
 * @date: 2024/3/12 16:26
 **/
public class Dws2Doris {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool parameterTool = EnvironmentUtils.createParameterTool();
        env.getConfig().setGlobalJobParameters(parameterTool);

        OutputTag<TagKafkaInfo> secondOutPutTag = new OutputTag<TagKafkaInfo>("side-output-second") {
        };
        SingleOutputStreamOperator<TagKafkaInfo> mainDS = env.addSource(MyKafkaUtil.getKafkaPojoConsumer(
                        parameterTool.get("kafka.dws.topic"),
                        "20240307",
                        parameterTool.get("kafka.server")))
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
                    public void processElement(TagKafkaInfo tagKafkaInfo, ProcessFunction<TagKafkaInfo, TagKafkaInfo>.Context context, Collector<TagKafkaInfo> collector) throws Exception {
                        String calculateParam = tagKafkaInfo.getCalculateParam();
                        if (calculateParam.contains("s")) {
                            context.output(secondOutPutTag, tagKafkaInfo);
                        } else {
                            collector.collect(tagKafkaInfo);
                        }
                    }
                });

        DataStream<TagKafkaInfo> secondDS = mainDS.getSideOutput(secondOutPutTag);
        secondDS.print();


        secondDS
                .map(new BeanToJsonStrFunction<>())
                .addSink(DorisUtil.getDorisSink(
                        parameterTool.get("doris.fenode"),
                        parameterTool.get("doris.username"),
                        parameterTool.get("doris.password"),
                        parameterTool.get("doris.second.table")
                ));

        mainDS
                .map(new BeanToJsonStrFunction<>())
                .addSink(DorisUtil.getDorisSink(
                        parameterTool.get("doris.fenode"),
                        parameterTool.get("doris.username"),
                        parameterTool.get("doris.password"),
                        parameterTool.get("doris.today.table")
                ));

        env.execute(Dws2Doris.class.getSimpleName());
    }
}

