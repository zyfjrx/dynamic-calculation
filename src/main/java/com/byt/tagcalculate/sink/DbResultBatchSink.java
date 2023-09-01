package com.byt.tagcalculate.sink;

import com.byt.tagcalculate.connection.impl.DataSourceGetter;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

/**
 * @title: 实时同步配置
 * @author: zhangyifan
 * @date: 2023/7/28 13:37
 */
@Slf4j
public class DbResultBatchSink extends RichSinkFunction<List<TagKafkaInfo>> {
    private transient SimpleDateFormat sdf;
    private PreparedStatement ps;
    private String tableName;
    private Connection connection;
    private ParameterTool parameterTool;

    public DbResultBatchSink(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        connection = DataSourceGetter.getMysqlDataSource(
                parameterTool.get("mysql.host"),
                parameterTool.getInt("mysql.port"),
                parameterTool.get("mysql.username"),
                parameterTool.get("mysql.password"),
                parameterTool.get("mysql.database")
        ).getConnection();
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
            log.info("DB Connection is closed");
        }
        super.close();
    }

    @Override
    public void invoke(List<TagKafkaInfo> value, Context context) throws Exception {


        String sql = "insert into " + tableName + "(byt_name, tag_topic, value, calculate_time, calculate_type,calculate_params, job_name, line_id) values(?, ?, ?, ?, ?, ?, ?, ?);";
        PreparedStatement ps = connection.prepareStatement(sql);

        for (TagKafkaInfo tag : value) {
            ps.setString(1, tag.getBytName());
            ps.setString(2, tag.getTopic());
            ps.setBigDecimal(3, tag.getValue());
            ps.setTimestamp(4, Timestamp.valueOf(tag.getTime()));
            ps.setString(5, tag.getCalculateType());
            ps.setString(6, tag.getCalculateParam());
            ps.setString(7, tag.getTaskName());
            ps.setInt(8, tag.getLineId());
            ps.addBatch();
        }


        try {
            int[] count = ps.executeBatch();
            System.out.println("inserted " + Arrays.stream(count).count() + " records");
            ps.clearBatch();
            connection.commit();
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
            connection.rollback();
            throw new RuntimeException(e);
        }
    }
}
