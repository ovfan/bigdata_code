package com.fanfan.flink.day03;

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
 * @ClassName: Example9_3
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月18日 08时32分
 * @Version: v1.0
 */
public class Example9_3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new IntSource())
                .setParallelism(1)
                .keyBy(r -> "int")
                .print();
        //.process(new MyStatistic())

        env.execute();
    }

    // PROCRESS 得逻辑代码
    public static class MyStatistic extends KeyedProcessFunction<String, IntStatistic, IntStatistic> {
        //使用值状态变量 维护累加器
        private ValueState<IntStatistic> accumulator;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 值状态变量的初始化
            accumulator = getRuntimeContext().getState(new ValueStateDescriptor<IntStatistic>(
                    "accumulator",
                    Types.POJO(IntStatistic.class)
            ));
        }

        @Override
        public void processElement(IntStatistic in, Context context, Collector<IntStatistic> out) throws Exception {

        }
    }

    //自定义数据源
    public static class IntSource implements SourceFunction<IntStatistic> {
        private boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<IntStatistic> ctx) throws Exception {
            int r;
            while (running) {
                r = random.nextInt(1000);
                ctx.collect(new IntStatistic(
                        r,
                        r,
                        r,
                        1,
                        r
                ));
            }
        }

        @Override
        public void cancel() {

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
