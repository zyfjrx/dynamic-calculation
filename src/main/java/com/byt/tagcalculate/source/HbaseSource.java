package com.byt.tagcalculate.source;


import com.byt.common.utils.TimeUtil;
import com.byt.tagcalculate.pojo.OdsTag;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @title:
 * @author: zhangyf
 * @date: 2024/3/20 09:21
 **/
public class HbaseSource extends RichSourceFunction<OdsTag> {
    private Connection connection = null;
    private ResultScanner rs = null;
    private Table table = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration hconf = HBaseConfiguration.create();
        hconf.set("hbase.zookeeper.quorum", "ai1:2181,ai2:2181,ai3:2181");
        connection = ConnectionFactory.createConnection(hconf);
    }

    @Override
    public void run(SourceContext<OdsTag> sourceContext) throws Exception {
        table = connection.getTable(TableName.valueOf("default:ods_data_1"));
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("info"),Bytes.toBytes("value"));
        rs = table.getScanner(scan);
        for (Result result : rs) {
            OdsTag odsTag = new OdsTag();
            StringBuilder sb = new StringBuilder();
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                sb.append(value);
            }
            String roeKey = new String(result.getRow());
            String tag = roeKey.substring(0, roeKey.length() - 15);
            String time = roeKey.substring(roeKey.length() - 14, roeKey.length());
            odsTag.setTime(TimeUtil.reformat2(time));
            odsTag.setTagName(tag);
            try {
                odsTag.setValue(Double.parseDouble(sb.toString()));
                sourceContext.collect(odsTag);
            }catch (Exception e){
                System.out.println(roeKey+"----->"+sb);
                e.printStackTrace();
            }

        }

    }

    @Override
    public void cancel() {
    }

    @Override
    public void close() throws Exception {
        if (rs != null) rs.close();
        if (table != null) table.close();
        if (connection != null) connection.close();
    }

}
