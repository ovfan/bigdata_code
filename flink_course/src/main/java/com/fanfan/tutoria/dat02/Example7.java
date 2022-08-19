package com.fanfan.tutoria.dat02;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


import java.util.Random;

/**
 * @ClassName: Example7
 * @Description: TODO 练习逻辑分区算子 reduce
 * 完成 整数流的历史统计功能
 * @Author: fanfan
 * @DateTime: 2022年08月19日 16时02分
 * @Version: v1.0
 */
public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .addSource(new IntegerSource())
                .setParallelism(1)
                .map(r -> Tuple5.of(r, r, r, 1, r))
                .returns(Types.TUPLE(Types.INT, Types.INT, Types.INT, Types.INT, Types.INT))
                .keyBy(r -> "int")
                .reduce(new Statistic())
                .print()
                .setParallelism(1);

        env.execute();
    }

    public static class Statistic implements ReduceFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>> {
        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Integer> reduce(Tuple5<Integer, Integer, Integer, Integer, Integer> acc, Tuple5<Integer, Integer, Integer, Integer, Integer> in) throws Exception {
            // 所有的算子都是被动执行，随着输入数据的到来驱动执行

            return Tuple5.of(
                    Math.min(in.f0, acc.f0),
                    Math.max(in.f1, acc.f1),
                    in.f2 + acc.f2,
                    in.f3 + acc.f3,
                    (in.f2 + acc.f2) / (in.f3 + acc.f3)
            );
        }
    }

    public static class IntegerSource implements SourceFunction<Integer> {
        private boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                ctx.collect(random.nextInt(1000));
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
