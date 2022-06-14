package com.fanfan.flink.java.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @ClassName: Example1_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月14日 13时37分
 * @Version: v1.0
 */
public class Example1_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);
        dataStreamSource.flatMap(new Tokenizer())
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    // r -> r.f0
                    @Override
                    public String getKey(Tuple2<String, Integer> in) throws Exception {

                        return in.f0;
                    }
                }).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> in, Tuple2<String, Integer> out) throws Exception {
                return Tuple2.of(in.f0, in.f1 + out.f1);
            }
        }).print().setParallelism(1);

        env.execute();
    }

    private static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = in.split("\\s+");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }
    }
}
