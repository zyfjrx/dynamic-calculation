package test;


import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/7/21 13:10
 **/
public class KalmanFilter {
    private RealMatrix X; // 状态向量
    private RealMatrix P; // 状态协方差矩阵
    private RealMatrix F; // 状态转移矩阵
    private RealMatrix Q; // 系统噪声协方差矩阵
    private RealMatrix H; // 观测矩阵
    private RealMatrix R; // 观测噪声协方差矩阵

    public KalmanFilter(RealMatrix initialState, RealMatrix initialCovariance,
                        RealMatrix stateTransition, RealMatrix processNoise,
                        RealMatrix observation, RealMatrix observationNoise) {
        this.X = initialState;
        this.P = initialCovariance;
        this.F = stateTransition;
        this.Q = processNoise;
        this.H = observation;
        this.R = observationNoise;
    }

    public void predict() {
        // 预测步骤
        X = F.multiply(X); // 更新状态向量
        P = F.multiply(P).multiply(F.transpose()).add(Q); // 更新状态协方差矩阵
    }

    public void update(RealMatrix measurement) {
        // 更新步骤
        RealMatrix I = MatrixUtils.createRealIdentityMatrix(X.getRowDimension()); // 单位矩阵
        RealMatrix y = measurement.subtract(H.multiply(X)); // 测量残差
        RealMatrix S = H.multiply(P).multiply(H.transpose()).add(R); // 测量残差协方差矩阵
        RealMatrix K = P.multiply(H.transpose()).multiply(MatrixUtils.inverse(S)); // 卡尔曼增益

        X = X.add(K.multiply(y)); // 更新状态向量
        P = (I.subtract(K.multiply(H))).multiply(P); // 更新状态协方差矩阵
    }



    public static void main(String[] args) {
        double[][] initialStateArray = {{0}};
        double[][] initialCovarianceArray = {{1}};
        double[][] stateTransitionArray = {{1}};
        double[][] processNoiseArray = {{0.0001}};
        double[][] observationArray = {{1}};
        double[][] observationNoiseArray = {{0.1}};

        RealMatrix initialState = MatrixUtils.createRealMatrix(initialStateArray);
        RealMatrix initialCovariance = MatrixUtils.createRealMatrix(initialCovarianceArray);
        RealMatrix stateTransition = MatrixUtils.createRealMatrix(stateTransitionArray);
        RealMatrix processNoise = MatrixUtils.createRealMatrix(processNoiseArray);
        RealMatrix observation = MatrixUtils.createRealMatrix(observationArray);
        RealMatrix observationNoise = MatrixUtils.createRealMatrix(observationNoiseArray);

        KalmanFilter kalmanFilter = new KalmanFilter(initialState, initialCovariance,
                stateTransition, processNoise, observation, observationNoise);

        double[] measurements = {1.2, 1.3, 1.5, 0.8, 0.9};
        for (double measurement : measurements) {
            RealMatrix measurementMatrix = MatrixUtils.createRealMatrix(new double[][]{{measurement}});
            kalmanFilter.predict();
            kalmanFilter.update(measurementMatrix);

            System.out.println("Filtered measurement: " + kalmanFilter.X.getEntry(0, 0));
        }
    }
}
