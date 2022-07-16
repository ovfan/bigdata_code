package com.fanfan.flink.day03_2;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example9
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月16日 20时16分
 * @Version: v1.0
 * TODO 练习物理分区算子--自定义分区
 * 将指定key的数据发送到下游的某一个并行子任务中
 */
public class Example9 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9)
                .setParallelism(1)
                .partitionCustom(
                        // 指定相同key的数据发往下游哪一个并行子任务中
                        new Partitioner<String>() {
                            @Override
                            public int partition(String key, int i) {
                                if (key == "奇数") {
                                    return 0;
                                } else {
                                    return 1;
                                }
                            }
                        },
                        // 指定key
                        new KeySelector<Integer, String>() {

                            @Override
                            public String getKey(Integer in) throws Exception {
                                String key;
                                if (in % 2 == 1) {
                                    key = "奇数";
                                } else {
                                    key = "偶数";
                                }
                                return key;
                            }
                        })
                .print()
                .setParallelism(4);

        env.execute();
    }
}
