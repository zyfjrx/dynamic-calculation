package com.byt.test;


import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.iotdb.flink.DefaultIoTSerializationSchema;
import org.apache.iotdb.flink.IoTDBSink;
import org.apache.iotdb.flink.options.IoTDBSinkOptions;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class FlinkIoTDBSink {
    public static void main(String[] args) throws Exception {
        // run the flink job on local mini cluster
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String host = "192.168.8.161";
        int port = 6667;
        String user = "root";
        String password = "root";

        IoTDBSinkOptions options = new IoTDBSinkOptions(
                host,
                port,
                user,
                password,
                Lists.newArrayList(
                        new IoTDBSinkOptions.TimeseriesOption(
                                "root.hk.line2.byt_name",
                                TSDataType.TEXT,
                                TSEncoding.GORILLA,
                                CompressionType.SNAPPY
                        ),
                        new IoTDBSinkOptions.TimeseriesOption(
                                "root.hk.line2.value",
                                TSDataType.DOUBLE,
                                TSEncoding.GORILLA,
                                CompressionType.SNAPPY
                        ),
                        new IoTDBSinkOptions.TimeseriesOption(
                                "root.hk.line2.calculate_time",
                                TSDataType.TEXT,
                                TSEncoding.GORILLA,
                                CompressionType.SNAPPY
                        )
                ));

        DefaultIoTSerializationSchema serializationSchema = new DefaultIoTSerializationSchema();
        IoTDBSink ioTDBSink = new IoTDBSink(options, serializationSchema)
                .withBatchSize(10)
                .withSessionPoolSize(3);

        env.addSource(new SensorSource())
                .name("sensor-source")
                .setParallelism(1)
                .addSink(ioTDBSink)
                .name("iotdb-sink");



        env.execute("iotdb-flink-example");
    }

    private static class SensorSource implements SourceFunction<Map<String, String>> {
        boolean running = true;
        Random random = new SecureRandom();
        private String[] tagNames = {"byt-a1","byt-a2"};

        @Override
        public void run(SourceContext context) throws Exception {
            while (running) {
                Map<String, String> tuple1 = new HashMap<>();
                tuple1.put("device", "root.hk.line2");
                tuple1.put("timestamp", String.valueOf(System.currentTimeMillis()));
                tuple1.put("measurements", "byt_name,value,calculate_time");
                tuple1.put("types", "TEXT,DOUBLE,TEXT");
                tuple1.put("values", tagNames[random.nextInt(tagNames.length)]+","+String.valueOf(random.nextInt(100))+","+reformat(System.currentTimeMillis()));

              /*  Map<String, String> tuple2 = new HashMap<>();
                tuple2.put("device", "root.sg.d1");
                tuple2.put("timestamp", String.valueOf(System.currentTimeMillis()));
                tuple2.put("measurements", "tag");
                tuple2.put("types", "TEXT");
                tuple2.put("values", tagNames[random.nextInt(tagNames.length)]);*/

                context.collect(tuple1);

                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static String reformat(long l) {
        //yyyyMMddHHmmss
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//可以方便地修改日期格式
        String s1 = "";
        try {
            s1 = sdf.format(new Date(l));

        } catch (Exception e) {
            System.out.println(e);
        }
        return s1;
    }
}
