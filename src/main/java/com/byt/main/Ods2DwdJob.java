package com.byt.main;

import com.alibaba.fastjson.JSON;
import com.byt.cdc.FlinkCDC;
import com.byt.constants.PropertiesConstants;
import com.byt.func.BroadcastProcessFunc;
import com.byt.pojo.TagKafkaInfo;
import com.byt.pojo.TagProperties;
import com.byt.utils.ConfigManager;
import com.byt.utils.MyKafkaUtilDev;
import com.byt.utils.StreamEnvUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;


/**
 * @title: 读取原始数据补充相关计算字段到DWD层
 * @author: zhangyf
 * @date: 2023/7/6 10:25
 */
public class Ods2DwdJob {
    public static void main(String[] args) throws Exception {
        // TODO 0.获取执行环境信息
        StreamExecutionEnvironment env = StreamEnvUtil.getEnv("ods2dwd");
        env.setParallelism(1);
        // TODO 1.定义广播状态描述器、读取配置流转换为广播流
        MapStateDescriptor<String, TagProperties> mapStateDescriptor = new MapStateDescriptor<>(
                "map-state",
                Types.STRING,
                Types.POJO(TagProperties.class));
        BroadcastStream<String> mysqlDS = env  // 读取配置流
                .fromSource(FlinkCDC.getMysqlSource(), WatermarkStrategy.noWatermarks(), "mysql")
                .broadcast(mapStateDescriptor);// 定义广播状态,将配置流进行广播

        // TODO 2.读取业务数据,连接广播流补充字段
        DataStreamSource<List<TagKafkaInfo>> kafkaDS = env
                .addSource(
                        MyKafkaUtilDev
                                .getKafkaListConsumerWM(ConfigManager.getListProperty("kafka.ods.topic"),
                                        ConfigManager.getProperty("kafka.group.id")
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
                                String string = JSON.toJSONString(tagKafkaInfo);
                                collector.collect(string);
                            }
                        }
                    }
                });

        // sink kafka
        resultDS.addSink(
                        MyKafkaUtilDev
                                .getKafkaSinkBySchema(new KafkaSerializationSchema<String>() {
                                    @Override
                                    public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long aLong) {
                                        String topic = ConfigManager.getProperty(PropertiesConstants.KAFKA_DWD_TOPIC_PREFIX);
                                        return new ProducerRecord<byte[], byte[]>(topic, s.getBytes(StandardCharsets.UTF_8));
                                    }
                                })
                )
                .name("sink to kafka");

        resultDS.print();
        // 启动job
        env.execute("dwd_arithmetic_job");
    }
}
