package com.fanfan.flink.day03;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @ClassName: Example9_2
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月17日 19时16分
 * @Version: v1.0
 * 复习Reduce的使用
 */
public class Example9_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .addSource(new IntSource())
                .setParallelism(1)
                .keyBy(r -> "int")
                .reduce(new ReduceFunction<IntStatisticPOJO>() {
                    @Override
                    public IntStatisticPOJO reduce(IntStatisticPOJO acc, IntStatisticPOJO in) throws Exception {
                        return new IntStatisticPOJO(
                                Math.max(acc.r1, in.r1),
                                Math.min(acc.r2, in.r2),
                                acc.r3 + in.r3,
                                in.r4 + acc.r4,
                                (acc.r3 + in.r3) / (acc.r4 + in.r4)
                        );
                    }
                })
                .print().setParallelism(1);

        env.execute();
    }

    public static class IntSource implements SourceFunction<IntStatisticPOJO> {
        private boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<IntStatisticPOJO> src) throws Exception {
            int r;
            while (running) {
                r = random.nextInt(1000);
                src.collect(new IntStatisticPOJO(
                        r,
                        r,
                        r,
                        1,
                        r
                ));
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class IntStatisticPOJO {
        public Integer r1;
        public Integer r2;
        public Integer r3;
        public Integer r4;
        public Integer r5;

        public IntStatisticPOJO() {
        }

        public IntStatisticPOJO(Integer r1, Integer r2, Integer r3, Integer r4, Integer r5) {
            this.r1 = r1;
            this.r2 = r2;
            this.r3 = r3;
            this.r4 = r4;
            this.r5 = r5;
        }

        @Override
        public String toString() {
            return "(" +
                    "最大值=" + r1 +
                    ", 最小值=" + r2 +
                    ", 总和=" + r3 +
                    ", 总条数=" + r4 +
                    ", 平均值=" + r5 +
                    ")";
        }
    }
}
