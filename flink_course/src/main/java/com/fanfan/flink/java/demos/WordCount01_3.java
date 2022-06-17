package com.fanfan.flink.java.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: WordCount01_3
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月17日 08时26分
 * @Version: v1.0
 */
public class WordCount01_3 {
    public static void main(String[] args) throws Exception {
        // 复习wordcount
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env
                .fromElements("hello scala", "hello flink")
                .flatMap((String str, Collector<Tuple2<String, Integer>> out) -> {
                    String[] words = str.split("\\s+");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy("f0")
                .sum("1")
                .print(" ")
                .setParallelism(4);


        env.execute();

    }
}
