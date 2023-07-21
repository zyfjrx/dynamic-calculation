package com.byt.tagcalculate.calculate.func;

import com.byt.common.utils.BytTagUtil;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * @title: 卡尔曼滤波器 kalman filter
 * @author: zhangyifan
 * @date: 2023/7/12 13:47
 */
public class KfProcessFunc extends KeyedProcessFunction<String, TagKafkaInfo, TagKafkaInfo> {

    private ValueState<Tuple2<RealMatrix, RealMatrix>> KFState;
    private OutputTag<TagKafkaInfo> dwdOutPutTag;


    private RealMatrix A; // 状态转移矩阵
    private RealMatrix Q; // 系统噪声协方差矩阵
    private RealMatrix H; // 观测矩阵
    private RealMatrix R; // 观测噪声协方差矩阵

    public KfProcessFunc(OutputTag<TagKafkaInfo> dwdOutPutTag) {
        this.dwdOutPutTag = dwdOutPutTag;
    }


    public void predict() throws IOException {
        Tuple2<RealMatrix, RealMatrix> kfs = KFState.value();
        RealMatrix X = kfs.f0;
        RealMatrix P = kfs.f1;
        // 预测步骤
        X = A.multiply(X); // 更新状态向量
        P = A.multiply(P).multiply(A.transpose()).add(Q); // 更新状态协方差矩阵

        kfs.setField(X, 0);
        kfs.setField(P, 1);
        KFState.update(kfs);
    }

    public void update(RealMatrix measurement) throws IOException {
        Tuple2<RealMatrix, RealMatrix> kfs = KFState.value();
        RealMatrix X = kfs.f0;
        RealMatrix P = kfs.f1;
        // 更新步骤
        RealMatrix I = MatrixUtils.createRealIdentityMatrix(X.getRowDimension()); // 单位矩阵
        RealMatrix y = measurement.subtract(H.multiply(X)); // 测量残差
        RealMatrix S = H.multiply(P).multiply(H.transpose()).add(R); // 测量残差协方差矩阵
        RealMatrix K = P.multiply(H.transpose()).multiply(MatrixUtils.inverse(S)); // 卡尔曼增益

        X = X.add(K.multiply(y)); // 更新状态向量
        P = (I.subtract(K.multiply(H))).multiply(P); // 更新状态协方差矩阵
        kfs.setField(X, 0);
        kfs.setField(P, 1);
        KFState.update(kfs);
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Tuple2<RealMatrix, RealMatrix>> KFStateDescriptor =
                new ValueStateDescriptor<Tuple2<RealMatrix, RealMatrix>>(
                        "KFState",
                        TypeInformation.of(new TypeHint<Tuple2<RealMatrix, RealMatrix>>() {
                        })
                );
        KFState = getRuntimeContext().getState(KFStateDescriptor);
    }

    @Override
    public void processElement(TagKafkaInfo value, KeyedProcessFunction<String, TagKafkaInfo, TagKafkaInfo>.Context ctx, Collector<TagKafkaInfo> out) throws Exception {
        double measurement = value.getValue().doubleValue();
        double[] measurements = {measurement};
        Double dt = value.getDt();
        Double r = value.getR();
        if (dt != null && r != null) {
            double nowValue = value.getValue().doubleValue();
            Tuple2<RealMatrix, RealMatrix> kfs = KFState.value();
            if (kfs == null) {
                RealMatrix X = MatrixUtils.createRealMatrix(new double[][]{{measurement}, {0}}); // 初始状态向量
                RealMatrix P = MatrixUtils.createRealMatrix(new double[][]{{1,0},{0,1}}); // 状态协方差矩阵
                A = MatrixUtils.createRealMatrix(new double[][]{{1, dt}, {0, 1}}); // 状态转换矩阵
                Q = MatrixUtils.createRealMatrix(new double[][]{{0.05, 0.05}, {0.05, 0.05}});
                H = MatrixUtils.createRealMatrix(new double[][]{{1,0}});
                R = MatrixUtils.createRealMatrix(new double[][]{{r}});
                KFState.update(Tuple2.of(X, P));
            }
            predict();
            double result = KFState.value().f0.getEntry(0, 0);
            RealMatrix measurementMatrix = MatrixUtils.createRealMatrix(new double[][]{measurements});
            update(measurementMatrix);
            value.setValue(new BigDecimal(result).setScale(4, BigDecimal.ROUND_HALF_UP));
            BytTagUtil.outputByKeyed(value, ctx, out, dwdOutPutTag);
            out.collect(value);
        }
    }
}