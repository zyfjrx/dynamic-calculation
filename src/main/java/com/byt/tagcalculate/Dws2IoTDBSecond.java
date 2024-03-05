package com.byt.tagcalculate;


import com.byt.common.utils.EnvironmentUtils;
import com.byt.common.utils.MyKafkaUtil;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;


public class Dws2IoTDBSecond {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境中,主题并行度与Kafka主题的分区数保持一致
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        ParameterTool parameterTool = EnvironmentUtils.createParameterTool();
        env.getConfig().setGlobalJobParameters(parameterTool);
        OutputTag<TagKafkaInfo> secondOutPutTag = new OutputTag<TagKafkaInfo>("side-output-second") {
        };
        SingleOutputStreamOperator<TagKafkaInfo> mainDS = env.addSource(MyKafkaUtil.getKafkaPojoConsumer(
                        parameterTool.get("kafka.dws.topic"),
                        "2024zyf",
                        parameterTool.get("kafka.server")))
                .process(new ProcessFunction<TagKafkaInfo, TagKafkaInfo>() {
                    @Override
                    public void processElement(TagKafkaInfo tagKafkaInfo, ProcessFunction<TagKafkaInfo, TagKafkaInfo>.Context context, Collector<TagKafkaInfo> collector) throws Exception {
                        String calculateParam = tagKafkaInfo.getCalculateParam();
                        if (calculateParam.contains("s")) {
                            context.output(secondOutPutTag, tagKafkaInfo);
                        } else {
                            collector.collect(tagKafkaInfo);
                        }
                    }
                });

        DataStream<TagKafkaInfo> second = mainDS.getSideOutput(secondOutPutTag);

        SingleOutputStreamOperator<RowData> dorisDS = second
                .map(new MapFunction<TagKafkaInfo, RowData>() {
                    @Override
                    public RowData map(TagKafkaInfo tagKafkaInfo) throws Exception {
                        GenericRowData genericRowData = new GenericRowData(4);
                        genericRowData.setField(0, tagKafkaInfo.getBytName());
                        genericRowData.setField(1, tagKafkaInfo.getValue());
                        genericRowData.setField(2, tagKafkaInfo.getCalculateType());
                        genericRowData.setField(3, tagKafkaInfo.getTime());
                        return genericRowData;
                    }
                });
        dorisDS.print();

        //flink rowdata‘s schema
        String[] fields = {"bytName", "value", "calculateType", "timestamp"};
        //LogicalType[] types = {LogicalType.VARCHAR(256), DataTypes.DECIMAL(8,4), DataTypes.VARCHAR(255), DataTypes.TIMESTAMP()};

        LogicalType[] types = {new VarCharType(), new DecimalType(8,4), new VarCharType(), new VarCharType()};
        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");

        dorisDS
                .addSink(
                        DorisSink.sink(
                                fields,
                                types,
                                new DorisExecutionOptions.Builder()
                                        .setBatchIntervalMs(10L)
                                        .setBatchSize(200)
                                        .setEnableDelete(false)
                                        .setMaxRetries(3)
                                        .setStreamLoadProp(properties)
                                        .build(),
                                new DorisOptions.Builder()
                                        .setFenodes("ai5:8030")
                                        .setUsername("root")
                                        .setPassword("000000")
                                        .setTableIdentifier("test_db.dws_today")
                                        .build())
                );


        env.execute();




    }
}
