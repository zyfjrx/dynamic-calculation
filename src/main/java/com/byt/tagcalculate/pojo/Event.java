package com.byt.tagcalculate.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Event {
    private String device;
    private Long timestamp;
    private List<String> measurements;
    private List<TSDataType> types;
    private List<Object> values;
}

