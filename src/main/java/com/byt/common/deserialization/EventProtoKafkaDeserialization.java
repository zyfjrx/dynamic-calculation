package com.byt.common.deserialization;

import com.byt.common.protos.TagKafkaInfoProto;
import com.byt.common.utils.BytTagUtil;
import com.byt.common.utils.TimeUtil;
import com.byt.tagcalculate.pojo.Event;
import com.byt.tagcalculate.pojo.OdsTag;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @title: probuf协议数据序列化与反序列化
 * @author: zhang
 * @date: 2022/8/16 14:08
 */
public class EventProtoKafkaDeserialization implements KafkaDeserializationSchema<Event> {

    // 是否流结束，比如读到一个key为end的字符串结束，这里不再判断，直接返回false 不结束
    @Override
    public boolean isEndOfStream(Event nextElement) {
        return false;
    }

    // 反序列化类
    @Override
    public Event deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        ArrayList<OdsTag> kafkaInfos = new ArrayList<>();
        byte[] data = record.value();
        long kafkaTimestamp = record.timestamp();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        //SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // proto协议解析
        ListValue protoList = ListValue.parseFrom(data);
        Event event = new Event();
        ArrayList<String> measurements = new ArrayList<>();
        ArrayList<TSDataType> types = new ArrayList<>();
        ArrayList<Object> values = new ArrayList<>();
        for (Value protoValue : protoList.getValuesList()) {
            TagKafkaInfoProto.TagKafkaInfo kafkaInfoProtos = TagKafkaInfoProto.TagKafkaInfo.parseFrom(protoValue.toByteArray());
            String pName = kafkaInfoProtos.getName();
            String pValue = kafkaInfoProtos.getValue();
            String pTime = kafkaInfoProtos.getTime();
            String strValue = kafkaInfoProtos.getValue();
            if (pName.equals("") || pValue.equals("") || pTime.equals("")) {
                continue;
            }


            try {
                event.setTimestamp(TimeUtil.getStartTimeStamp(pTime));
                if (pName.contains("/")){
                    String newTagName = pName.replace("/", "_");
                    measurements.add(newTagName);
                }else {
                    measurements.add(kafkaInfoProtos.getName());
                }
                types.add(TSDataType.DOUBLE);


                if (pValue.equalsIgnoreCase("true") || pValue.equalsIgnoreCase("false")) {
                    //OdsTag.setValue(strValue);
                    if (strValue.equalsIgnoreCase("true")) {
                        values.add(Double.valueOf(1));
                    } else {
                        values.add(Double.valueOf(0));
                    }
                } else {
                    // 保留4位小数
                    BigDecimal v = new BigDecimal(kafkaInfoProtos.getValue()).setScale(4, BigDecimal.ROUND_HALF_UP);
                    values.add(Double.valueOf(v.toString()));
                }
            } catch (Exception e) {
                System.out.println("proto error: ");
                System.out.println("name: " + kafkaInfoProtos.getName());
                System.out.println("value: " + kafkaInfoProtos.getValue().equals(""));
                System.out.println("time: " + kafkaInfoProtos.getTime());
            }

        }
        event.setMeasurements(measurements);
        event.setTypes(types);
        event.setValues(values);
        return event;
    }

    // 用于获取反序列化对象的类型
    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(new TypeHint<Event>() {
        });
    }
}
