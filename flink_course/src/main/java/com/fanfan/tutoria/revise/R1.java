package com.fanfan.tutoria.revise;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: R1
 * @Description: TODO 复习wordcount
 * @Author: fanfan
 * @DateTime: 2022年08月25日 09时20分
 * @Version: v1.0
 */
public class R1 {
    public static void main(String[] args) throws Exception {
        // 1. 准备流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 定义数据流的DAG
        env
                .readTextFile("flink_course/src/main/resources/word.txt")
                .flatMap(new FlatMapFunction<String, CountPOJO>() {
                    @Override
                    public void flatMap(String in, Collector<CountPOJO> out) throws Exception {
                        String[] words = in.split(" ");
                        for (String word : words) {
                            out.collect(new CountPOJO(word, 1L));
                        }
                    }
                })
                .keyBy(new KeySelector<CountPOJO, String>() {
                    @Override
                    public String getKey(CountPOJO countPOJO) throws Exception {
                        return countPOJO.word;
                    }
                }).reduce(new ReduceFunction<CountPOJO>() {
            @Override
            public CountPOJO reduce(CountPOJO acc, CountPOJO in) throws Exception {
                return new CountPOJO(
                        in.word,
                        acc.count + in.count
                );
            }
        })
                .print();

        // 3. 提交并执行程序
        env.execute();
    }
}
