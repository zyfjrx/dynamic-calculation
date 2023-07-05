package com.byt.test;

import com.byt.constants.PropertiesConstants;
import com.byt.utils.ConfigManager;
import com.byt.utils.MyKafkaUtilDev;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.Properties;

/**
 * @title: 导入测试数据
 * @author: zhang
 * @date: 2022/6/23 08:34
 */
public class KafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

/*        FlinkKafkaConsumer<List<TagKafkaInfo>> kafkaConsumer = MyKafkaUtil.getKafkaConsumer("byt-south-hknf-lubrication", "aabb");
        kafkaConsumer.setStartFromTimestamp(1655280594220L);*/

        Properties properties = new Properties();
        Properties properties2 = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ConfigManager.getProperty(PropertiesConstants.KAFKA_SERVER));
        properties2.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ConfigManager.getProperty(PropertiesConstants.KAFKA_SERVER));


   /*     FlinkKafkaConsumerBase<String> kafkaConsumer = MyKafkaUtilDev
                .getKafkaConsumer("opc_test_data", "work_test02")
                .setStartFromTimestamp(1656485759622L);*/
        FlinkKafkaConsumer<String> consumer1 = new FlinkKafkaConsumer<>("ods_test_data", new SimpleStringSchema(), properties2);
        // consumer.setStartFromTimestamp(1657171601720L);
        consumer1.setStartFromEarliest();


        FlinkKafkaConsumer<String> consumer2 = new FlinkKafkaConsumer<>("ods_test_data1", new SimpleStringSchema(), properties2);
        // consumer.setStartFromTimestamp(1657171601720L);
        consumer2.setStartFromEarliest();

        env
                .addSource(consumer1)
                .map(new RichMapFunction<String, String>() {
                    @Override
                    public String map(String s) throws Exception {
                        Thread.sleep(ConfigManager.getLong(PropertiesConstants.SENT_TIME));
                        return s;
                    }
                }).addSink(new FlinkKafkaProducer<String>("ods_test_data1", new SimpleStringSchema(), properties))
                .name("发送周期：" + ConfigManager.getLong(PropertiesConstants.SENT_TIME).toString())
                .disableChaining();

        env
                .addSource(consumer2)
                .map(new RichMapFunction<String, String>() {
                    @Override
                    public String map(String s) throws Exception {
                        Thread.sleep(ConfigManager.getLong(PropertiesConstants.SENT_TIME));
                        return s;
                    }
                })
                .addSink(new FlinkKafkaProducer<String>("ods_test_data", new SimpleStringSchema(), properties))
                .disableChaining()
                .name("发送周期：" + ConfigManager.getLong(PropertiesConstants.SENT_TIME).toString());
        env.execute("mock_data");
    }
}


