package com.byt.tagcalculate;

import com.byt.common.utils.EnvironmentUtils;
import com.byt.common.utils.MyKafkaUtil;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;


public class BatchKafka2MysqlJob {
    private static ThreadLocal<DateFormat> pattern = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss"));

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = EnvironmentUtils.createParameterTool();
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setParallelism(1);
/*        long startTimestamp = pattern.get().parse("2024-02-29 14:55:00").getTime();
        long stopTimestamp = pattern.get().parse("2024-02-29 15:00:00").getTime();*/
        long startTimestamp = pattern.get().parse(args[0]).getTime();
        long stopTimestamp = pattern.get().parse(args[1]).getTime();
        SingleOutputStreamOperator<TagKafkaInfo> kafkaSource = env
                .addSource(MyKafkaUtil.getKafkaPojoConsumer(
                        parameterTool.get("kafka.dwd.topic"),
                        "test2_20230808",
                        parameterTool.get("kafka.server"),
                        startTimestamp,
                        stopTimestamp
                ))
                .filter(new FilterFunction<TagKafkaInfo>() {
                    @Override
                    public boolean filter(TagKafkaInfo tagKafkaInfo) throws Exception {
                        return tagKafkaInfo.getCalculateParam().contains("m");
                    }
                });

        kafkaSource.print();
        kafkaSource
                .addSink(JdbcSink.sink(
                        "insert into " + args[2] + "(byt_name, tag_topic, value, calculate_time, calculate_type,calculate_params, job_name, line_id) values(?, ?, ?, ?, ?, ?, ?, ?);",
                        (ps, tag) -> {
                            ps.setString(1, tag.getBytName());
                            ps.setString(2, tag.getTopic());
                            ps.setBigDecimal(3, tag.getValue());
                            ps.setTimestamp(4, Timestamp.valueOf(tag.getTime()));
                            ps.setString(5, tag.getCalculateType());
                            ps.setString(6, tag.getCalculateParam());
                            ps.setString(7, tag.getTaskName());
                            ps.setInt(8, tag.getLineId());
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchIntervalMs(1000L)
                                .withBatchSize(500)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://" + parameterTool.get("mysql.host") + ":" + parameterTool.getInt("mysql.port") + "/" + parameterTool.get("mysql.database") + "?autoReconnect=true&useSSL=false&queryTimeout=2400&characterEncoding=utf-8&serverTimezone=GMT%2B8")
                                .withUsername(parameterTool.get("mysql.username"))
                                .withPassword(parameterTool.get("mysql.password"))
                                .build()
                ));

        env.execute();
    }
}
