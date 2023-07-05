package com.byt.maintest;

import com.byt.calculate.TStream;
import com.byt.calculate.window.AVG;
import com.byt.constants.PropertiesConstants;
import com.byt.pojo.TagKafkaInfo;
import com.byt.utils.ConfigManager;
import com.byt.utils.MyKafkaUtilDev;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @title: 实现标签动态计算
 * @author: zhangyifan
 * @date: 2022/10/26 09:30
 */
public class Dwd2DwsDynamicArithmeticJob {
    public static void main(String[] args) throws Exception {
        // TODO 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 定义测输出标签
        OutputTag<TagKafkaInfo> errorSide = new OutputTag<TagKafkaInfo>("error-side-out") {
        };
        OutputTag<TagKafkaInfo> preOutPutTag = new OutputTag<TagKafkaInfo>("side-output-pre") {
        };
        OutputTag<TagKafkaInfo> avgOutPutTag = new OutputTag<TagKafkaInfo>("avg-output") {
        };
        OutputTag<TagKafkaInfo> minOutPutTag = new OutputTag<TagKafkaInfo>("min-output") {
        };
        OutputTag<TagKafkaInfo> maxOutPutTag = new OutputTag<TagKafkaInfo>("max-output") {
        };

        SingleOutputStreamOperator<TagKafkaInfo> mainStream = env
                // 2.1 添加数据源
                .addSource(MyKafkaUtilDev.getKafkaPojoConsumerWM(
                        ConfigManager.getProperty(PropertiesConstants.KAFKA_DWD_TOPIC_PREFIX) + 1,
                        ConfigManager.getProperty(PropertiesConstants.KAFKA_GROUP_ID))
                )
                .process(new ProcessFunction<TagKafkaInfo, TagKafkaInfo>() {
                    private SimpleDateFormat sdf1;
                    private SimpleDateFormat sdf2;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
                    }

                    @Override
                    public void processElement(TagKafkaInfo value, ProcessFunction<TagKafkaInfo, TagKafkaInfo>.Context context, Collector<TagKafkaInfo> collector) throws Exception {
                        String calculate = getCalculateParamByPOJO(value);
                        value.setTime(sdf1.format(sdf2.parse(value.getTime())));
                        if (calculate.contains("MAX")) {
                            context.output(maxOutPutTag, value);
                        } else if (calculate.contains("MIN")) {
                            context.output(minOutPutTag, value);
                        } else if (calculate.contains("AVG")) {
                            context.output(minOutPutTag, value);
                        }
                    }

                    public String getCalculateParamByPOJO(TagKafkaInfo data) {
                        // return data.getCalculateType() + "#" + data.getTimeGap() + "#" + data.getSlideGap();
                        return data.getCalculateType() + "#" + data.getCalculateParam();
                    }
                });
        new AVG(Arrays.asList(new String[]{"5s","1s"})).avg(mainStream.getSideOutput(avgOutPutTag),"5s","1s");


        // TODO 提交作业
        env.execute();
    }
}
