package com.fanfan.flink.java.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example3_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月14日 16时02分
 * @Version: v1.0
 */
public class Example3_1 {
    public static void main(String[] args) throws Exception {
        //Flink 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements("Hello flink", "hello flink", "hello spark")
                .flatMap(new Tokenizer())
                .keyBy(wordcount -> wordcount.word)
                .reduce(new Sum())
                .setParallelism(1)
                .print()
                .setParallelism(1);

        env.execute();
    }

    public static class Sum implements ReduceFunction<WorldCount> {
        @Override
        public WorldCount reduce(WorldCount in, WorldCount acc) throws Exception {

            return new WorldCount(in.word, in.count + acc.count);
        }
    }

    public static class Tokenizer implements FlatMapFunction<String, WorldCount> {
        @Override
        public void flatMap(String in, Collector<WorldCount> out) throws Exception {
            String[] words = in.split("\\s+");
            for (String word : words) {
                out.collect(new WorldCount(word, 1));
            }
        }
    }

    public static class WorldCount {
        public String word;
        public Integer count;

        public WorldCount() {
        }

        public WorldCount(String word, Integer count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WorldCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
