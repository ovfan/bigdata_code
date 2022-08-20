package com.fanfan.tutoria.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


import java.util.Random;

/**
 * @ClassName: Example4_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月20日 13时46分
 * @Version: v1.0
 */
public class Example4_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new SourceFunction<Integer>() {
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
                })
                .setParallelism(1)
                .map(new MapFunction<Integer, IntStatistic>() {

                    @Override
                    public IntStatistic map(Integer in) throws Exception {
                        return new IntStatistic(
                                in, in, in, 1, in
                        );
                    }
                })
                .setParallelism(1)
                .keyBy(r -> "int")
                .reduce(new ReduceFunction<IntStatistic>() {
                    @Override
                    public IntStatistic reduce(IntStatistic acc, IntStatistic in) throws Exception {
                        return new IntStatistic(
                                Math.min(in.min, acc.min),
                                Math.max(in.max, acc.max),
                                in.sum + acc.sum,
                                in.count + acc.count,
                                (in.sum + acc.sum) / (in.count + acc.count)
                        );
                    }
                })
                .setParallelism(1)
                .print()
                .setParallelism(1);

        env.execute();
    }

    public static class IntStatistic {
        public Integer min;
        public Integer max;
        public Integer sum;
        public Integer count;
        public Integer avg;

        public IntStatistic() {
        }

        public IntStatistic(Integer min, Integer max, Integer sum, Integer count, Integer avg) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
            this.avg = avg;
        }

        @Override
        public String toString() {
            return "(最小值:" + min + " 最大值:" + " 总和:" + sum + " 个数:" + count + " 平均值:" + avg + ")";
        }
    }
}
