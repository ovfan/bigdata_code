package com.fanfan.tutoria.day03;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example2
 * @Description: TODO 自定义分区 的用法
 * @Author: fanfan
 * @DateTime: 2022年08月20日 09时08分
 * @Version: v1.0
 */
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .setParallelism(1)
                .partitionCustom(new Partitioner<Integer>() {
                    // 用来设定将哪一个key的数据发送到哪一个并行子任务
                    @Override
                    public int partition(Integer key, int i) {
                        if (key == 0) {
                            return 0;
                        } else if (key == 1) {
                            return 1;
                        } else {
                            return 2;
                        }
                    }
                }, new KeySelector<Integer, Integer>() {
                    // 第二个参数用来指定输入数据的key
                    @Override
                    public Integer getKey(Integer in) throws Exception {
                        return in % 3;
                    }
                })
                .print("自定义分区")
                .setParallelism(4);


        env.execute();
    }
}
