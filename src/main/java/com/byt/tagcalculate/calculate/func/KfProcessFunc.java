package com.byt.tagcalculate.calculate.func;

import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.common.utils.BytTagUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.inverse.InvertMatrix;

import java.io.IOException;
import java.math.BigDecimal;

import static org.nd4j.linalg.api.buffer.DataType.DOUBLE;

/**
 * @title: 卡尔曼滤波器 kalman filter
 * @author: zhangyifan
 * @date: 2023/7/12 13:47
 */
public class KfProcessFunc extends KeyedProcessFunction<String,TagKafkaInfo,TagKafkaInfo> {

    private transient ValueState<Tuple2<INDArray, INDArray>> KFState;
    private OutputTag<TagKafkaInfo> dwdOutPutTag;
    Double dt;
    Double r;
    INDArray F;
    INDArray H;
    INDArray Q;
    INDArray R;

    public KfProcessFunc(OutputTag<TagKafkaInfo> dwdOutPutTag) {
        this.dwdOutPutTag = dwdOutPutTag;
    }

    private INDArray predict() throws IOException {
        Tuple2<INDArray, INDArray> kfs = KFState.value();
        INDArray x = kfs.f1;
        INDArray P = kfs.f0;
        x = F.mmul(x);
        P = F.mmul(P).mmul(F.transpose()).add(Q);
        kfs.setField(P, 0);
        kfs.setField(x, 1);
        KFState.update(kfs);
        return x;
    }

    private void update(double z) throws IOException {
        Tuple2<INDArray, INDArray> kfs = KFState.value();
        INDArray P = kfs.f0;
        INDArray x = kfs.f1;

        INDArray y = Nd4j.create(new double[]{z}).sub(H.mmul(x).reshape(1));
        INDArray S = R.add(H.mmul(P.mmul(H.transpose())));
        INDArray K = P.mmul(H.transpose()).mmul(InvertMatrix.invert(S, false));
        x = x.add(K.mmul(y));
        INDArray I = Nd4j.eye(2).castTo(DOUBLE);
        P = I.sub(K.mmul(H)).mmul(P).mmul(I.sub(K.mmul(H)).transpose()).add(K.mmul(R).mmul(K.transpose()));
        kfs.setField(P, 0);
        kfs.setField(x, 1);
        KFState.update(kfs);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Tuple2<INDArray, INDArray>> KFStateDescriptor =
                new ValueStateDescriptor<Tuple2<INDArray, INDArray>>(
                        "KFState",
                        TypeInformation.of(new TypeHint<Tuple2<INDArray, INDArray>>() {
                        })
                );
        KFState = getRuntimeContext().getState(KFStateDescriptor);
    }

    @Override
    public void processElement(TagKafkaInfo value, KeyedProcessFunction<String, TagKafkaInfo, TagKafkaInfo>.Context ctx, Collector<TagKafkaInfo> out) throws Exception {
        dt = value.getDt();
        r = value.getR();
        if (dt != null && r != null){
            double nowValue = value.getValue().doubleValue();
            Tuple2<INDArray, INDArray> kfs = KFState.value();
            if (kfs == null) {
                F = Nd4j.create(new double[][]{{1, this.dt}, {0, 1}});
                H = Nd4j.create(new double[][]{{1, 0}});
                Q = Nd4j.create(new double[][]{{0.05, 0.05}, {0.05, 0.05}});
                R = Nd4j.create(new double[][]{{this.r}});
                long n = F.shape()[1];
                INDArray P = Nd4j.eye((int) n).castTo(DOUBLE);
                INDArray x = Nd4j.create(new double[]{value.getValue().doubleValue(), 0});
                KFState.update(Tuple2.of(P, x));
            }
            double pValue = H.mmul(predict()).getDouble(0);
            update(nowValue);
            value.setValue(new BigDecimal(pValue).setScale(4, BigDecimal.ROUND_HALF_UP));
            BytTagUtil.outputByKeyed(value,ctx,out,dwdOutPutTag);
            out.collect(value);
        }

    }
}
