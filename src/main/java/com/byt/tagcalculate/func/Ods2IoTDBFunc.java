package com.byt.tagcalculate.func;

import com.byt.common.utils.TimeUtil;
import com.byt.tagcalculate.pojo.OdsTag;
import com.byt.tagcalculate.pojo.OdsTags;
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
public class Ods2IoTDBFunc extends ProcessFunction<List<OdsTag>, OdsTags> {
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
    public void processElement(List<OdsTag> odsTagList, ProcessFunction<List<OdsTag>, OdsTags>.Context context, Collector<OdsTags> collector) throws Exception {

        OdsTags odsTags = new OdsTags();
        for (OdsTag odsTag : odsTagList) {
            if (odsTag.getTagName().contains("/")) {
                String tagName = odsTag.getTagName();
                String newName = tagName.replace("/", "_");
                measurementsList.add(Collections.singletonList(newName));
            }else {
                measurementsList.add(Collections.singletonList(odsTag.getTagName()));
            }
            valuesList.add(Collections.singletonList(odsTag.getValue()));
            typesList.add(Collections.singletonList(TSDataType.DOUBLE));
            timestamps.add(TimeUtil.getStartTime(odsTag.getTime()));
        }
        odsTags.setTagNames(measurementsList);
        odsTags.setTimestamps(timestamps);
        odsTags.setValues(valuesList);
        odsTags.setTypes(typesList);
        collector.collect(odsTags);
        timestamps.clear();
        measurementsList.clear();
        valuesList.clear();
        typesList.clear();
    }
}
