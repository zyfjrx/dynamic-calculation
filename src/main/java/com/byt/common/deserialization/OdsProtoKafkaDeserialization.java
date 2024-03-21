package com.byt.common.deserialization;

import com.byt.common.protos.TagKafkaInfoProto;
import com.byt.common.utils.BytTagUtil;
import com.byt.tagcalculate.pojo.OdsTag;
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
public class OdsProtoKafkaDeserialization implements KafkaDeserializationSchema<List<OdsTag>> {

    // 是否流结束，比如读到一个key为end的字符串结束，这里不再判断，直接返回false 不结束
    @Override
    public boolean isEndOfStream(List<OdsTag> nextElement) {
        return false;
    }

    // 反序列化类
    @Override
    public List<OdsTag> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        ArrayList<OdsTag> kafkaInfos = new ArrayList<>();
        byte[] data = record.value();
        long kafkaTimestamp = record.timestamp();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        //SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // proto协议解析
        ListValue protoList = ListValue.parseFrom(data);
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
                OdsTag OdsTag = new OdsTag();
                OdsTag.setTagName(kafkaInfoProtos.getName());
                OdsTag.setTopic(record.topic());
                // 统一时间格式
                // 解决opc发送时间延迟问题，如果相差超过1000ms则以实际写入kafka的时间为准，否则以原始时间为准
                long tagTimestamp = sdf.parse(pTime).getTime();
                if (Math.abs(kafkaTimestamp-tagTimestamp) > 10000L){
                    //OdsTag.setTimestamp(kafkaTimestamp);
                    OdsTag.setTime(BytTagUtil.reformat(kafkaTimestamp));
                }else {
                    //OdsTag.setTimestamp(tagTimestamp);
                    OdsTag.setTime(BytTagUtil.formatTime(pTime));
                }
                if (pValue.equalsIgnoreCase("true") || pValue.equalsIgnoreCase("false")) {
                    //OdsTag.setValue(strValue);
                    if (strValue.equalsIgnoreCase("true")) {
                        OdsTag.setValue(BigDecimal.ONE);
                    } else {
                        OdsTag.setValue(BigDecimal.ZERO);
                    }
                    kafkaInfos.add(OdsTag);
                } else {
                    // 保留4位小数
                    BigDecimal v = new BigDecimal(kafkaInfoProtos.getValue()).setScale(4, BigDecimal.ROUND_HALF_UP);
                    OdsTag.setValue(v);
                    kafkaInfos.add(OdsTag);
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
    public TypeInformation<List<OdsTag>> getProducedType() {
        return TypeInformation.of(new TypeHint<List<OdsTag>>() {
        });
    }
}
