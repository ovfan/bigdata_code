package com.fanfan.tutoria.revise;

import com.fanfan.flink.day04.Example2_1;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: MyPartitionerExample
 * @Description: TODO 练习物理分区算子 -- 自定义分区
 * @Author: fanfan
 * @DateTime: 2022年08月25日 10时38分
 * @Version: v1.0
 */
public class MyPartitionerExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new Example2_1.IntSource())
                // - 第一个参数用来设定将哪一个key的数据发送到哪一个并行子任务
                //    					 - 第二个参数用来指定输入数据的key
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int i) {
                        int partitionNum;
                        if (key == 1 || key == 2) {
                            partitionNum = 0;
                        } else if (key == 3 || key == 4) {
                            partitionNum = 1;
                        } else {
                            partitionNum = 2;
                        }
                        return partitionNum;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer in) throws Exception {
                        return in % 7;
                    }
                })
                .print()
                .setParallelism(4);


        env.execute();
    }
}
