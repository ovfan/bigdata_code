package com.fanfan.tutoria.dat02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example8_2
 * @Description: TODO 自定义分区
 * @Author: fanfan
 * @DateTime: 2022年08月19日 17时43分
 * @Version: v1.0
 */
public class Example8_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements("1 2 3 4 5 6 7 8 9")
                .flatMap(new FlatMapFunction<String, Integer>() {
                    @Override
                    public void flatMap(String in, Collector<Integer> out) throws Exception {
                        String[] Integers = in.split("\\s+");
                        for (String i : Integers) {
                            out.collect(Integer.parseInt(i));
                        }
                    }
                })
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int i) {
                        if (key == 1 || key == 2) {
                            return 1;
                        } else {
                            return 2;
                        }
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer in) throws Exception {
                        return in % 3;
                    }
                })
                .print()
                .setParallelism(4);
        env.execute();
    }
}
