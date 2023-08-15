package com.byt.tagcalculate;

import com.alibaba.fastjson.JSONObject;
import com.byt.common.cdc.FlinkCDC;
import com.byt.common.utils.ConfigManager;
import com.byt.common.utils.EnvironmentUtils;
import com.byt.common.utils.MyKafkaUtil;
import com.byt.tagcalculate.constants.PropertiesConstants;
import com.byt.tagcalculate.func.BroadcastProcessFunc;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.tagcalculate.pojo.TagProperties;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;


/**
 * @title: 读取原始数据补充相关计算字段到DWD层
 * @author: zhangyf
 * @date: 2023/7/6 10:25
 */
public class Ods2DwdJob {
    public static void main(String[] args) throws Exception {
        // TODO 0.获取执行环境信息
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool parameterTool = EnvironmentUtils.createParameterTool();
        //env.setStateBackend(new EmbeddedRocksDBStateBackend());
        // TODO 1.定义广播状态描述器、读取配置流转换为广播流
        MapStateDescriptor<String, TagProperties> mapStateDescriptor = new MapStateDescriptor<>(
                "map-state",
                Types.STRING,
                Types.POJO(TagProperties.class));
        BroadcastStream<String> mysqlDS = env  // 读取配置流
                .fromSource(FlinkCDC.getMysqlSource(
                        parameterTool.get("mysql.host"),
                        parameterTool.getInt("mysql.port"),
                        parameterTool.get("mysql.username"),
                        parameterTool.get("mysql.password"),
                        parameterTool.get("mysql.database"),
                        parameterTool.get("mysql.table")
                ), WatermarkStrategy.noWatermarks(), "mysql")
                .broadcast(mapStateDescriptor);// 定义广播状态,将配置流进行广播

        // TODO 2.读取业务数据,连接广播流补充字段
        DataStreamSource<List<TagKafkaInfo>> kafkaDS = env
                .addSource(
                        MyKafkaUtil
                                .getKafkaListConsumer(Arrays.asList(parameterTool.get("kafka.ods.topic").split(",")),
                                        "test1_20230808", parameterTool.get("kafka.server")
                                ));
        // 连接两个流 connect()
        SingleOutputStreamOperator<String> resultDS = kafkaDS
                .connect(mysqlDS)
                .process(new BroadcastProcessFunc(mapStateDescriptor))
                .flatMap(new FlatMapFunction<List<TagKafkaInfo>, String>() {
                    @Override
                    public void flatMap(List<TagKafkaInfo> tagKafkaInfos, Collector<String> collector) throws Exception {
                        for (TagKafkaInfo tagKafkaInfo : tagKafkaInfos) {
                            if (tagKafkaInfo.getStatus() == 1) {
                                //tagKafkaInfo.setTime(BytTagUtil.reformat(tagKafkaInfo.getTimestamp()));
                                String jsonString = JSONObject.toJSONString(tagKafkaInfo);
                                collector.collect(jsonString);
                            }
                        }
                    }
                });

        // sink kafka
        resultDS.addSink(MyKafkaUtil.getKafkaProducer(parameterTool.get("kafka.dwd.topic"),parameterTool.get("kafka.server")))
                .name("sink to kafka");

        resultDS.print();
        // 启动job
        env.execute("dwd_arithmetic_job");
    }
}