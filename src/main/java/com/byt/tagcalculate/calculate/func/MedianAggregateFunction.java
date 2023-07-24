package com.byt.tagcalculate.calculate.func;

import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @title: 增量聚合
 * @author: zhangyf
 * @date: 2023/7/24 13:54
 **/
public class MedianAggregateFunction implements AggregateFunction<TagKafkaInfo, Tuple2<TagKafkaInfo, List<BigDecimal>>, TagKafkaInfo> {


    @Override
    public Tuple2<TagKafkaInfo, List<BigDecimal>> createAccumulator() {
        return Tuple2.of(new TagKafkaInfo(), new ArrayList<>());
    }

    @Override
    public Tuple2<TagKafkaInfo, List<BigDecimal>> add(TagKafkaInfo tagKafkaInfo, Tuple2<TagKafkaInfo, List<BigDecimal>> acc) {
        acc.f0 = tagKafkaInfo;
        acc.f1.add(tagKafkaInfo.getValue());
        return acc;
    }

    @Override
    public TagKafkaInfo getResult(Tuple2<TagKafkaInfo, List<BigDecimal>> acc) {
        acc.f1.sort(new Comparator<BigDecimal>() {
            @Override
            public int compare(BigDecimal o1, BigDecimal o2) {
                return o1.subtract(o2).intValue();
            }
        });
        BigDecimal median = new BigDecimal(0L);
        if (acc.f1.size() % 2 == 0) {
            median = acc.f1.get(acc.f1.size() / 2 - 1)
                    .add(acc.f1.get(acc.f1.size() / 2))
                    .divide(BigDecimal.valueOf(2L), 4, BigDecimal.ROUND_HALF_UP);
        } else {
            median = acc.f1.get((acc.f1.size() + 1) / 2 - 1);
        }
        acc.f0.setValue(median);
        return acc.f0;
    }

    @Override
    public Tuple2<TagKafkaInfo, List<BigDecimal>> merge(Tuple2<TagKafkaInfo, List<BigDecimal>> acc1, Tuple2<TagKafkaInfo, List<BigDecimal>> acc2) {
        acc1.f1.addAll(acc2.f1);
        return Tuple2.of(acc1.f0, acc1.f1);
    }
}
