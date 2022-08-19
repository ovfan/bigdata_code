package com.fanfan.tutoria.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: WordCount2
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月19日 08时37分
 * @Version: v1.0
 */
public class WordCount2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile("flink_course/src/main/resources/info.properties");
        Integer port = parameterTool.getInt("port");
        System.out.println("端口号为:  = " + port);
        env.setParallelism(1);
        env
                .socketTextStream("hadoop102", port)
                .flatMap(new Tokenizer())
                .keyBy(r -> r.f0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> acc, Tuple2<String, Integer> in) throws Exception {
                        return Tuple2.of(in.f0, acc.f1 + in.f1);
                    }
                })
                .print();


        env.execute();
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = in.split("\\s+");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }

        }
    }
}
