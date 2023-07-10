package com.byt.common.deserialization;

import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @title:
 * @author: zhangyifan
 * @date: 2022/8/27 16:15
 */
public class TagInfoDeserializationSchema implements DeserializationSchema<TagKafkaInfo> {
    public static final long serialVersionUID = 1L;
    public static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public TagKafkaInfo deserialize(byte[] bytes) throws IOException {
        TagKafkaInfo tagKafkaInfo = null;
        try {
            tagKafkaInfo = objectMapper.readValue(bytes, TagKafkaInfo.class);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("转换 TagKafkaInfo 数据类型异常，输入数据非法");
        }
        return tagKafkaInfo;
    }

    @Override
    public boolean isEndOfStream(TagKafkaInfo tagKafkaInfo) {
        return false;
    }

    @Override
    public TypeInformation<TagKafkaInfo> getProducedType() {
        return TypeInformation.of(TagKafkaInfo.class);
    }
}
