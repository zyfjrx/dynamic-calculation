package com.byt.tagcalculate.func;

import com.byt.common.utils.TimeUtil;
import com.byt.tagcalculate.pojo.OdsTag;
import com.byt.tagcalculate.pojo.OdsTagRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @title:
 * @author: zhangyf
 * @date: 2024/3/24 15:11
 **/
public class Ods2IoTDBFunc extends ProcessFunction<List<OdsTag>, OdsTagRecord> {
    private List<Long> timestamps;
    private List<List<String>> measurementsList;
    private List<List<TSDataType>> typesList;
    private List<List<Object>> valuesList;

    @Override
    public void open(Configuration parameters) throws Exception {
        timestamps = new ArrayList<>();
        measurementsList = new ArrayList<>();
        typesList = new ArrayList<>();
        valuesList = new ArrayList<>();
    }

    @Override
    public void processElement(List<OdsTag> odsTagList, ProcessFunction<List<OdsTag>, OdsTagRecord>.Context context, Collector<OdsTagRecord> collector) throws Exception {

        OdsTagRecord odsTagRecord = new OdsTagRecord();
        for (OdsTag odsTag : odsTagList) {

            String tagName = odsTag.getTagName();
            //String newName = tagName.replaceAll("[/:.]", "_");
            String newName = "`"+tagName+"`";
            measurementsList.add(Collections.singletonList(newName));

            valuesList.add(Collections.singletonList(odsTag.getValue()));
            System.out.println(odsTag);
            typesList.add(Collections.singletonList(TSDataType.DOUBLE));
            timestamps.add(TimeUtil.getStartTime(odsTag.getTime()));
        }
        odsTagRecord.setTagNames(measurementsList);
        odsTagRecord.setTimestamps(timestamps);
        odsTagRecord.setValues(valuesList);
        odsTagRecord.setTypes(typesList);
        collector.collect(odsTagRecord);
        timestamps.clear();
        measurementsList.clear();
        valuesList.clear();
        typesList.clear();
    }
}
