package com.byt.tagcalculate.main;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.byt.common.cdc.FlinkCDC;
import com.byt.common.utils.EnvironmentUtils;
import com.byt.common.utils.MyKafkaUtil;
import com.byt.tagcalculate.func.AsyncTagsPost;
import com.byt.tagcalculate.func.Descriptors;
import com.byt.tagcalculate.func.PostJsonFunc;
import com.byt.tagcalculate.func.Value2PNameAndValue;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @title:
 * @author: zhangyifan
 * @date: 2022/9/27 16:10
 */
public class Dws2PostJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool parameterTool = EnvironmentUtils.createParameterTool();
        // 读取配置流数据
        SingleOutputStreamOperator<JSONObject> cdcPost = env
                .fromSource(FlinkCDC.getMysqlSourceWithPost(), WatermarkStrategy.noWatermarks(),"mysql")
                .map(JSON::parseObject);
        cdcPost.print("cdc>>>>>>>>");
        // 定义广播状态描述符
        BroadcastStream<JSONObject> broadcastDim = cdcPost.broadcast(Descriptors.postDimDescriptor);
        BroadcastStream<JSONObject> broadcastJson = cdcPost.broadcast(Descriptors.postMapDescriptor);

        // 第一次广播，为数据划分属于的postName
        SingleOutputStreamOperator<Tuple2<String, TagKafkaInfo>> broadcast1 = env
                .addSource(MyKafkaUtil.getKafkaPojoConsumer(
                        parameterTool.get("kafka.dws.topic"),
                        "test2_20230808",
                        parameterTool.get("kafka.server")
                        ))
                .connect(broadcastDim)
                .process(new Value2PNameAndValue());
        broadcast1.print("1>>>>>>>>");


        // 第二次广播为数据补充发送的url
        SingleOutputStreamOperator<Tuple2<String, String>> postDS = broadcast1
                .keyBy(x -> x.f0) // 同一个post_name的数据在一起处理
                .connect(broadcastJson)
                .process(new PostJsonFunc());
        postDS.print("2<<<<<<<<<<");

        postDS.map(new MapFunction<Tuple2<String, String>, String>() {
            @Override
            public String map(Tuple2<String, String> tuple2) throws Exception {
                return tuple2.f1;
            }
        }).addSink(MyKafkaUtil.getKafkaProducer(parameterTool.get("kafka.dws.topic"),parameterTool.get("kafka.server")));
        AsyncDataStream.orderedWait(postDS,new AsyncTagsPost(),10000, TimeUnit.MILLISECONDS,100);
        env.execute("post_arithmetic_job");
    }
}
