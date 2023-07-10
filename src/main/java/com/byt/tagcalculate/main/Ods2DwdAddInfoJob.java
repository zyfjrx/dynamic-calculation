package com.byt.tagcalculate.main;

import com.byt.common.cdc.FlinkCDC;
import com.byt.tagcalculate.constants.PropertiesConstants;
import com.byt.tagcalculate.func.DWDBroadcastProcessFunc;
import com.byt.tagcalculate.func.FilterAndFlatMap;
import com.byt.tagcalculate.pojo.TagProperties;
import com.byt.common.utils.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;


/**
 * @title: 读取原始数据补充相关计算字段到DWD层
 * @author: zhang
 * @date: 2022/8/15 10:25
 */
@Deprecated
public class Ods2DwdAddInfoJob {
    public static void main(String[] args) throws Exception {
        // TODO 0.获取执行环境信息
        StreamExecutionEnvironment env = StreamEnvUtil.getEnv("ods2dwd");
        env.setParallelism(1);
        // TODO 1.定义广播状态描述器、读取配置流转换为广播流
        MapStateDescriptor<String, TagProperties> mapStateDescriptor = new MapStateDescriptor<>(
                "map-state",
                Types.STRING,
                Types.POJO(TagProperties.class)
        );
        BroadcastStream<String> mysqlSource = env  // 读取配置流
                .fromSource(FlinkCDC.getMysqlSource(), WatermarkStrategy.noWatermarks(),"mysql")
                .setParallelism(1)
                .broadcast(mapStateDescriptor);// 定义广播状态,将配置流进行广播

        // TODO 2.读取业务数据,连接广播流补充字段
        SingleOutputStreamOperator<String> resultDS = env
                .addSource(
                        MyKafkaUtilDev
                                //.getKafkaListConsumerWM(ConfigManager.getListProperty(PropertiesConstants.KAFKA_ODS_TOPIC),
                                .getKafkaListConsumerWM(ConfigManager.getListProperty("kafka.ods.topic"),
                                        ConfigManager.getProperty("kafka.group.id")
                                ))
                .setParallelism(1)
                // 连接两个流 connect()
                .connect(mysqlSource)
                .process(new DWDBroadcastProcessFunc(mapStateDescriptor))
                .flatMap(new FilterAndFlatMap()).name("kakfa")
                .name("连接广播流");

        // TODO 3.sink to kafka dwd
        // todo dev
        //resultDS.addSink(MyKafkaUtilDev.getKafkaProducer(ConfigManager.getProperty(PropertiesConstants.KAFKA_DWD_DEFAULT_TOPIC)));
        resultDS.addSink(
                        MyKafkaUtilDev
                                .getKafkaSinkBySchema(new KafkaSerializationSchema<String>() {
                                    @Override
                                    public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long aLong) {
                                        //System.out.println("--------"+s);
                                        //JSONObject jsonObject = JSON.parseObject(s);
                                        //String lineId = jsonObject.getString("lineId");
                                        //String topic = ConfigManager.getProperty(PropertiesConstants.KAFKA_DWD_TOPIC_PREFIX) + lineId;
                                        String topic = ConfigManager.getProperty(PropertiesConstants.KAFKA_DWD_TOPIC_PREFIX);
                                        //System.out.println("topic:" + topic);
                                        return new ProducerRecord<byte[], byte[]>(topic, s.getBytes(StandardCharsets.UTF_8));
                                    }
                                })
                )
                .name("sink to kafka");
        // 打印测试
        resultDS.print();
        // 启动job
        env.execute("dwd_arithmetic_job");
    }
}
