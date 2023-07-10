package com.byt.tagcalculate.func;

import com.byt.tagcalculate.calculate.calculatechain.TStream;
import com.byt.tagcalculate.calculate.calculatechain.Transform;
import com.byt.tagcalculate.calculate.calculatechain.TransformChain;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

import java.lang.reflect.Constructor;
import java.util.*;

/**
 * @title: 核心计算函数
 * @author: zhangyifan
 * @date: 2022/7/6 09:18
 */
public class CoreCalculateFunction {

    public static List<DataStream<TagKafkaInfo>> coreCalculate(Set<String> calPs,
                                                               SingleOutputStreamOperator<TagKafkaInfo> mainStream,
                                                               HashMap<String, OutputTag<TagKafkaInfo>> sideOutPutTags) throws Exception {
        ArrayList<DataStream<TagKafkaInfo>> streamArray = new ArrayList<>();

        for (String p : calPs) {
            System.out.println("-------------------------------------------------");
            System.out.println("calParams:" + p);
            OutputTag<TagKafkaInfo> outputTag = sideOutPutTags.get(p);
            DataStream<TagKafkaInfo> sideOutputStresam = mainStream.getSideOutput(outputTag);
            System.out.println(sideOutputStresam);

            String[] ps = p.split("#");
            String calculates = ps[0].toUpperCase();
            String[] cals = calculates.split("_");

            String params = ps[1];
            String[] timeOrParams = params.split("\\|");

            TransformChain chain = new TransformChain();
            for (int i = 0; i < cals.length; i++) {
                String[] split = timeOrParams[i].split(",");
                List<String> list = Arrays.asList(split);
                // 通过类反射动态调用相应的计算算子
                Class<?> aClass = Class.forName("com.byt.tagcalculate.calculate.window." + cals[i]);
                Constructor<?> constructor = aClass.getConstructor();
                System.out.println("calculates:"+calculates);
                Transform calClass = (Transform) constructor.newInstance();
                chain.add(calClass);
                System.out.println(cals[i]+"->"+timeOrParams[i]);
            }
            TStream tStream = new TStream(sideOutputStresam);
            chain.doTransform(tStream);
            streamArray.add(tStream.stream);
        }
        /*for (String p : params) {
            System.out.println("calParams:" + p);
            OutputTag<TagKafkaInfo> outputTag = sideOutPutTags.get(p);
            DataStream<TagKafkaInfo> sideOutputStresam = mainStream.getSideOutput(outputTag);
            System.out.println(sideOutputStresam);
            String[] ps = p.split("#");
            String calculate = ps[0].toUpperCase();
            ArrayList<Tuple2<String, String>> pTimeGap = new ArrayList<>();
            // ArrayList<Tuple3< String,String,String>> params = new ArrayList<>();
            if (ps.length == 3) {
                // AVG_SUM#1,2#6
                String[] timeGaps = ps[1].split(",");
                for (String timeGap : timeGaps) {
                    pTimeGap.add(Tuple2.of(timeGap, ps[2]));
                }
            } else if (ps.length > 1) {
                //1,2|2,3
                //(1,2)  (2,3)
                String[] timeGaps = ps[1].split("\\|");
                for (String timeGap : timeGaps) {
                    String[] split = timeGap.split(",");
                    pTimeGap.add(Tuple2.of(split[0], split[1]));
                }
            } else {
                pTimeGap.add(Tuple2.of("5", "5"));
            }

            String[] cals = calculate.split("_");
            TransformChain chain = new TransformChain();
            for (int i = 0; i < cals.length; i++) {
                // 通过类反射动态调用相应的计算算子
                Class<?> aClass = Class.forName("com.byt.tagcalculate.calculate.window." + cals[i]);
                Constructor<?> constructor = aClass.getConstructor(Tuple2.class);
                Transform calClass = (Transform) constructor.newInstance(pTimeGap.get(i));
                chain.add(calClass);
            }*/
           /* TStream tStream = new TStream(sideOutputStresam);
            chain.doTransform(tStream);
            streamArray.add(tStream.stream);
        }*/


        return streamArray;
    }

    public static DataStream<TagKafkaInfo> unionStream(List<DataStream<TagKafkaInfo>> streamArray){
        DataStream<TagKafkaInfo> outPutStream = null;
        for (DataStream<TagKafkaInfo> stream : streamArray) {
            if (outPutStream == null) {
                outPutStream = stream;
            } else {
                outPutStream = outPutStream.union(stream);
            }
        }
        return outPutStream;
    }
}
