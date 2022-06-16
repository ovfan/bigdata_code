package com.fanfan.flink.day02;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @ClassName: Example11_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月17日 00时45分
 * @Version: v1.0
 */
public class Example11_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<Integer>() {
                    private Random random = new Random();
                    private boolean running = true;
                    @Override
                    public void run(SourceContext<Integer> sourceContext) throws Exception {
                        while (running) {
                            sourceContext.collect(random.nextInt(1000));
                            Thread.sleep(1000L);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .map(r -> Tuple5.of(r, r, r, 1, r))
                .returns(Types.TUPLE(
                        Types.INT,
                        Types.INT,
                        Types.INT,
                        Types.INT,
                        Types.INT
                ))
                .keyBy(r -> "int")
                .reduce(new Statistic())
                .print();

        env.execute();
    }

    public static class Statistic implements ReduceFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>> {
        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Integer> reduce(Tuple5<Integer, Integer, Integer, Integer, Integer> in, Tuple5<Integer, Integer, Integer, Integer, Integer> acc) throws Exception {
            return Tuple5.of(
                    Math.min(in.f0, acc.f0), // 最小值
                    Math.max(in.f1, acc.f1), // 最大值
                    in.f2 + acc.f2,          // 总和
                    in.f3 + acc.f3,              // 总条数
                    (in.f2 + acc.f2) / (in.f3 + acc.f3) // 平均值
            );
        }
    }
}
