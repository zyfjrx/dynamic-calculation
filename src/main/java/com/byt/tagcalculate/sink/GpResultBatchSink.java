package com.byt.tagcalculate.sink;

import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.common.utils.ConfigManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class GpResultBatchSink extends RichSinkFunction<List<TagKafkaInfo>> {
    PreparedStatement ps;
    private Connection connection;
    private String tableName;
    private int invokeCount;


    public GpResultBatchSink(String tableName) {
        this.tableName = tableName;
    }

    private static Connection getConnection() throws Exception {
        Connection con = null;
        Class.forName("org.postgresql.Driver");
        con = DriverManager.getConnection(
                ConfigManager.getProperty("greenplum.url"),
                ConfigManager.getProperty("greenplum.user"),
                ConfigManager.getProperty("greenplum.pass"));
        return con;
    }

    private PreparedStatement getPS() throws Exception {
        String sql = "insert into " + tableName + "(byt_name, tag_topic, value, calculate_time, calculate_type,calculate_params, job_name, line_id) values(?, ?, ?, ?, ?, ?, ?, ?);";
        if (connection != null) {
            ps = this.connection.prepareStatement(sql);
        }
        return ps;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        ps = getPS();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    @Override
    public void invoke(List<TagKafkaInfo> value, Context context) throws Exception {
        if (ps == null) {
            return;
        }
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
            invokeCount++;
        } catch (SQLException e) {
            System.out.println("写数据到gp异常");
        }
        // 批次超过900000000条数据关闭链接
        if (invokeCount == 900000000) {
            close();
            connection = getConnection();
            ps = getPS();
            invokeCount = 0;
        }
    }
}
