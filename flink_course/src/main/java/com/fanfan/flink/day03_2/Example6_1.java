package com.fanfan.flink.day03_2;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example6_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月16日 08时40分
 * @Version: v1.0
 */
public class Example6_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .socketTextStream("hadoop102", 9999)
                .process(new MyProcess())
                .keyBy(r -> r.f0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> acc, Tuple2<String, Integer> in) throws Exception {
                        Integer res = in.f1 + acc.f1;
                        return Tuple2.of(in.f0, res);
                    }
                })
                .print();

        env.execute();
    }

    public static class MyProcess extends ProcessFunction<String, Tuple2<String, Integer>> {
        @Override
        public void open(Configuration parameters) throws Exception {

        }

        @Override
        public void processElement(String in, Context context, Collector<Tuple2<String, Integer>> out) throws Exception {
            if (in.equals("white")) {
                out.collect(Tuple2.of(in, 1));
            } else if (in.equals("black")) {
                out.collect(Tuple2.of(in, 2));
            } else {
                out.collect(Tuple2.of(in, 3));
            }
        }

        @Override
        public void close() throws Exception {

        }
    }
}
