package com.byt.tagcalculate.pojo;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

@Data
@NoArgsConstructor
public class Event {
    private String device;
    private Long timestamp;
    private List<String> measurements;
    private List<TSDataType> types;
    private List<Object> values;

    public Event(
            String device,
            Long timestamp,
            List<String> measurements,
            List<TSDataType> types,
            List<Object> values) {
        this.device = device;
        this.timestamp = timestamp;
        this.measurements = measurements;
        this.types = types;
        this.values = values;
    }

    public List<TSDataType> getTypes() {
        return types;
    }

    public void setTypes(List<TSDataType> types) {
        this.types = types;
    }

    public String getDevice() {
        return device;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public List<String> getMeasurements() {
        return measurements;
    }

    public List<Object> getValues() {
        return values;
    }

    @Override
    public String toString() {
        return "Event{" +
                "device='" + device + '\'' +
                ", timestamp=" + timestamp +
                ", measurements=" + measurements +
                ", types=" + types +
                ", values=" + values +
                '}';
    }
}

