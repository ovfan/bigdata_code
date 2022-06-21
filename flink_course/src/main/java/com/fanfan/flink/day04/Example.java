package com.fanfan.flink.day04;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @ClassName: Example
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月18日 09时23分
 * @Version: v1.0
 */
public class Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new IntSource())
                .setParallelism(1)
                .keyBy(r -> "int")
                .process(new MyStatistic())
                //.addSink(new MyJDBC())
                .print();
        env.execute();
    }
    public static class MyJDBC extends RichSinkFunction<IntStatistic>{

    }
    public static class MyStatistic extends KeyedProcessFunction<String, Integer, IntStatistic> {
        // 需要数据的统计值累加器和定时器值状态变量
        private ValueState<IntStatistic> acc;
        private ValueState<Integer> flag;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化累加器
            acc = getRuntimeContext().getState(new ValueStateDescriptor<IntStatistic>(
                    "ACC",
                    Types.POJO(IntStatistic.class)
            ));
            flag = getRuntimeContext().getState(new ValueStateDescriptor<Integer>(
                    "FLAG",
                    Types.INT
            ));
        }

        @Override
        public void processElement(Integer in, Context context, Collector<IntStatistic> out) throws Exception {
            // 数据流来的第一条数据
            // TODO 只更新累加器值状态变量 -- 不collect输出数据
            if (acc.value() == null) {
                acc.update(new IntStatistic(in, in, in, 1, in));
            } else {
                IntStatistic oldAcc = acc.value();
                IntStatistic newAcc = new IntStatistic(
                        Math.max(in, oldAcc.r1),
                        Math.min(in, oldAcc.r2),
                        in + oldAcc.r3,
                        1 + oldAcc.r4,
                        (in + oldAcc.r3) / (1 + oldAcc.r4)
                );
                acc.update(newAcc);
            }


            // 只有不存在定时器的情况下才注册定时器
            // Integer value = flag.value();
            // 如果为空，则注册定时器
            if (flag.value() == null) {
                // 获取当前机器时间
                long currTs = context.timerService().currentProcessingTime();
                context.timerService().registerProcessingTimeTimer(currTs + 10 * 1000L);
                // 将标志位置设置 非空；
                flag.update(1);
            }

        }

        // 定时器用来向下游发送统计结果
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<IntStatistic> out) throws Exception {
            out.collect(acc.value());
            flag.clear();
        }
    }

    public static class IntSource implements SourceFunction<Integer> {
        private boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<Integer> out) throws Exception {
            int r;
            while (running) {
                r = random.nextInt(1000);
                out.collect(r);
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class IntStatistic {
        public Integer r1;
        public Integer r2;
        public Integer r3;
        public Integer r4;
        public Integer r5;

        public IntStatistic() {
        }

        public IntStatistic(Integer r1, Integer r2, Integer r3, Integer r4, Integer r5) {
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

