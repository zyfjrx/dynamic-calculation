package test;

import com.byt.common.utils.MyKafkaUtilDev;
import lombok.EqualsAndHashCode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @title:
 * @author: zhangyifan
 * @date: 2022/10/11 14:12
 */
@EqualsAndHashCode
public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(MyKafkaUtilDev.getKafkaPojoConsumerWM("dwd_data","sssaaaa"))
                        .print();

env.execute();
    }
}
