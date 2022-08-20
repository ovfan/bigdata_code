package com.fanfan.tutoria.day03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example2_2
 * @Description: TODO 自定义分区
 * @Author: fanfan
 * @DateTime: 2022年08月20日 09时20分
 * @Version: v1.0
 */
public class Example2_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .readTextFile("flink_course/src/main/resources/Integer.txt")
                .setParallelism(1)
                .flatMap(new FlatMapFunction<String, Integer>() {
                    @Override
                    public void flatMap(String in, Collector<Integer> out) throws Exception {
                        String[] numbers = in.split("\\s+");
                        for (String number : numbers) {
                            int n = Integer.parseInt(number);
                            out.collect(n);
                        }
                    }
                })
                .partitionCustom(new Partitioner<Integer>() {
                    // numPartitions 算子并行度
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        if (key == 0 || key == 1) {
                            return 0;
                        } else {
                            return 1;
                        }
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer in) throws Exception {
                        return in % 3;
                    }
                }).print()
                .setParallelism(4);


        env.execute();
    }
}
