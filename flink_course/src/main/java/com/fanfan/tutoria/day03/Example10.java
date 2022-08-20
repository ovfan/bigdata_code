package com.fanfan.tutoria.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @ClassName: Example9
 * @Description: TODO 使用KeyedProcessFunction 内部状态-值状态变量 实现 整数流的 统计
 * 实现限流统计
 * @Author: fanfan
 * @DateTime: 2022年08月20日 16时11分
 * @Version: v1.0
 */
public class Example10 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new IntegetSource())
                .map(new MapFunction<Integer, IntStreamStatistic>() {

                    @Override
                    public IntStreamStatistic map(Integer in) throws Exception {
                        return new IntStreamStatistic(
                                in, in, in, 1, in
                        );
                    }
                })
                .keyBy(r -> "int")
                .process(new KeyedProcessFunction<String, IntStreamStatistic, IntStreamStatistic>() {

                    private ValueState<IntStreamStatistic> accumulator; // 声明累加器值状态变量
                    private ValueState<Boolean> flag; // 标志位，标志定时器是否存在

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // TODO 初始化值状态变量
                        accumulator = getRuntimeContext().getState(new ValueStateDescriptor<IntStreamStatistic>(
                                "acc",
                                Types.POJO(IntStreamStatistic.class)
                        ));
                        flag = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>(
                                "flag",
                                Types.BOOLEAN
                        ));
                    }

                    @Override
                    public void processElement(IntStreamStatistic in, Context ctx, Collector<IntStreamStatistic> out) throws Exception {
                        // 当第一条数据到来的时候，作为值状态变量保存下来
                        if (accumulator.value() == null) {
                            accumulator.update(new IntStreamStatistic(
                                    in.min, in.max, in.sum, in.count, in.avg
                            ));
                        } else {
                            // 获取值状态变量的值作为 旧的累加器
                            IntStreamStatistic oldAcc = accumulator.value();
                            IntStreamStatistic newAcc = new IntStreamStatistic(
                                    Math.min(oldAcc.min, in.min),
                                    Math.max(oldAcc.max, in.max),
                                    oldAcc.sum + in.sum,
                                    oldAcc.count + in.count,
                                    (oldAcc.sum + in.sum) / (oldAcc.count + in.count)
                            );

                            accumulator.update(newAcc);
                        }
                        // 当定时器 的 值状态变量为空时，才注册定时器
                        if (flag.value() == null){
                            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10 * 1000L);
                            flag.update(true);
                        }

                    }

                    @Override
                    public void onTimer(long ts, OnTimerContext ctx, Collector<IntStreamStatistic> out) throws Exception {
                        out.collect(accumulator.value());

                        // 定时器执行时候，清空标志位的值状态变量
                        flag.clear();
                    }
                })
                .print();

        env.execute();
    }

    public static class IntStreamStatistic {
        public Integer min;
        public Integer max;
        public Integer sum;
        public Integer count;
        public Integer avg;

        public IntStreamStatistic() {

        }

        public IntStreamStatistic(Integer min, Integer max, Integer sum, Integer count, Integer avg) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
            this.avg = avg;
        }

        @Override
        public String toString() {
            return "{" +
                    "min=" + min +
                    ", max=" + max +
                    ", sum=" + sum +
                    ", count=" + count +
                    ", avg=" + avg +
                    '}';
        }
    }

    public static class IntegetSource implements SourceFunction<Integer> {
        private Boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                int randomNum = random.nextInt(1000);
                ctx.collect(randomNum);
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}

