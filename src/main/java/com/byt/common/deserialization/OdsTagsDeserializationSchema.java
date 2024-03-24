package com.byt.common.deserialization;

import com.byt.tagcalculate.pojo.OdsTags;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @title:
 * @author: zhangyifan
 * @date: 2022/8/27 16:15
 */
public class OdsTagsDeserializationSchema implements DeserializationSchema<OdsTags> {
    public static final long serialVersionUID = 1L;
    public static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public OdsTags deserialize(byte[] bytes) throws IOException {
        OdsTags odsTags = null;
        try {
            odsTags = objectMapper.readValue(bytes, OdsTags.class);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("转换 TagKafkaInfo 数据类型异常，输入数据非法");
        }
        return odsTags;
    }

    @Override
    public boolean isEndOfStream(OdsTags odsTags) {
        return false;
    }

    @Override
    public TypeInformation<OdsTags> getProducedType() {
        return TypeInformation.of(OdsTags.class);
    }
}
