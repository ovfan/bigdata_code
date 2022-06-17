package com.fanfan.flink.day03;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example2
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月17日 09时48分
 * @Version: v1.0
 * //TODO 使用自定义分区算子(partitionCustom) 将数据路由到指定的并行子任务中执行
 */
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .setParallelism(1)
                .partitionCustom(
                        new Partitioner<Integer>() {
                            /**
                             * 获取数据的key 来决定数据前往哪个partition
                             * @param key
                             * @param i
                             * @return
                             * 需求:key 为 0、1的数据前往第一个partition中
                             *      key为 2的数据前往第二个partition中
                             *      */
                            @Override
                            public int partition(Integer key, int i) {
                                int partitionNum;
                                if (key == 0 || key == 1) {
                                    partitionNum = 0;
                                } else {
                                    partitionNum = 1;
                                }
                                return partitionNum;
                            }
                        },
                        // TODO 数据 % 3 的值作为数据的key
                        new KeySelector<Integer, Integer>() {
                            @Override
                            public Integer getKey(Integer in) throws Exception {
                                return in % 3;
                            }
                        })
                .print("使用自定义分区算子(partitionCustom) 将数据路由到指定的并行子任务中执行")
                .setParallelism(4);
        env.execute();
    }
}
