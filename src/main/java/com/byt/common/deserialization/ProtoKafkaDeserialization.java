package com.byt.common.deserialization;

import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.common.protos.TagKafkaInfoProto;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
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
public class ProtoKafkaDeserialization implements KafkaDeserializationSchema<List<TagKafkaInfo>> {

    // 是否流结束，比如读到一个key为end的字符串结束，这里不再判断，直接返回false 不结束
    @Override
    public boolean isEndOfStream(List<TagKafkaInfo> nextElement) {
        return false;
    }

    // 反序列化类
    @Override
    public List<TagKafkaInfo> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        ArrayList<TagKafkaInfo> kafkaInfos = new ArrayList<>();
        byte[] data = record.value();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        //SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // proto协议解析
        ListValue protoList = ListValue.parseFrom(data);
        for (Value protoValue : protoList.getValuesList()) {
            TagKafkaInfoProto.TagKafkaInfo kafkaInfoProtos = TagKafkaInfoProto.TagKafkaInfo.parseFrom(protoValue.toByteArray());
            String pName = kafkaInfoProtos.getName();
            String pValue = kafkaInfoProtos.getValue();
            String pTime = kafkaInfoProtos.getTime();
            if (pName.equals("") || pValue.equals("") || pTime.equals("")) {
                continue;
            }

            try {
//                TagKafkaProtos.TagKafkaInfo kafkaInfoProtos = TagKafkaProtos.TagKafkaInfo.parseFrom(value.toByteArray());
                TagKafkaInfo tagKafkaInfo = new TagKafkaInfo();
                String strValue = kafkaInfoProtos.getValue();
                tagKafkaInfo.setName(kafkaInfoProtos.getName());
                //tagKafkaInfo.setTime(kafkaInfoProtos.getTime());
                tagKafkaInfo.setTopic(record.topic());
                //System.out.println(record.topic()+"-----------------------");
                // TODO dev
                tagKafkaInfo.setTime(kafkaInfoProtos.getTime());
                tagKafkaInfo.setTimestamp(sdf.parse(tagKafkaInfo.getTime()).getTime());
                //tagKafkaInfo.setTimestamp(System.currentTimeMillis());
                //tagKafkaInfo.setTime(sdf2.format(new Timestamp(System.currentTimeMillis())));
                if (strValue.equalsIgnoreCase("true") || strValue.equalsIgnoreCase("false")) {
                    tagKafkaInfo.setStrValue(strValue);
                    if (strValue.equalsIgnoreCase("true")) {
                        tagKafkaInfo.setValue(BigDecimal.ONE);
                    } else {
                        tagKafkaInfo.setValue(BigDecimal.ZERO);
                    }
                    kafkaInfos.add(tagKafkaInfo);
                } else {
                    // TODO 保留两位小数
                    BigDecimal v = new BigDecimal(kafkaInfoProtos.getValue()).setScale(4, BigDecimal.ROUND_HALF_UP);
                    tagKafkaInfo.setValue(v);
                    kafkaInfos.add(tagKafkaInfo);
                }
            } catch (Exception e) {
                System.out.println("proto error: ");
                System.out.println("name: " + kafkaInfoProtos.getName());
                System.out.println("value: " + kafkaInfoProtos.getValue().equals(""));
                System.out.println("time: " + kafkaInfoProtos.getTime());
            }

        }
        return kafkaInfos;
    }

    // 用于获取反序列化对象的类型
    @Override
    public TypeInformation<List<TagKafkaInfo>> getProducedType() {
        return TypeInformation.of(new TypeHint<List<TagKafkaInfo>>() {
        });
    }
}
