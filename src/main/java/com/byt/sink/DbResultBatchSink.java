package com.byt.sink;

import com.byt.connection.SinkConnection;
import com.byt.connection.impl.DbConnection;
import com.byt.pojo.TagKafkaInfo;
import com.byt.utils.ConfigManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

@Slf4j
public class DbResultBatchSink extends RichSinkFunction<List<TagKafkaInfo>> {
    PreparedStatement ps;
    private String tableName;
    private SinkConnection sinkConnection;

    public DbResultBatchSink(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if(sinkConnection == null) {
            sinkConnection = new DbConnection();
            log.info("DB Connection is open");
        }
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        if(sinkConnection != null) {
            sinkConnection.close();
            log.info("DB Connection is closed");
        }
        super.close();
    }

    @Override
    public void invoke(List<TagKafkaInfo> value, Context context) throws Exception {


        Connection con = sinkConnection.getConnection();
        String sql = "insert into " + tableName + "(byt_name, tag_topic, value, calculate_time, calculate_type,calculate_params, job_name, line_id) values(?, ?, ?, ?, ?, ?, ?, ?);";
        PreparedStatement ps = con.prepareStatement(sql);

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
            con.commit();
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
            con.rollback();
            throw new RuntimeException(e);
        }
    }
}
