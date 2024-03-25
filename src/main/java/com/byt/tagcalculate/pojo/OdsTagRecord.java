package com.byt.tagcalculate.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

/**
 * @title:
 * @author: zhangyf
 * @date: 2024/3/20 10:36
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OdsTagRecord {
    private List<List<String>> tagNames;
    private List<String> deviceIds;
    private String time;
    private List<Long> timestamps;
    private String topic;
    private String strValue;
    private List<List<Object>> values;
    private List<List<TSDataType>> types;
}
