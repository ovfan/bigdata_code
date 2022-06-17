package com.fanfan.flink.day03;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @ClassName: Example9_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月17日 18时51分
 * @Version: v1.0
 * TODO 自定义Int数据源，利用值状态变量求出(最大值，最小值，累加值，出现条数，平均值)
 */
public class Example9_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new IntSource())
                .keyBy(r -> "int")
                .process(new MyIntStatistic())
                .print();

        env.execute();
    }

    public static class MyIntStatistic extends KeyedProcessFunction<String,IntStatistic,IntStatistic>{
        private ValueState<IntStatistic> accumulator;

        // 初始化值状态变量
        @Override
        public void open(Configuration parameters) throws Exception {
            accumulator = getRuntimeContext().getState(
                    new ValueStateDescriptor<IntStatistic>(
                            "accumulator",
                            Types.POJO(IntStatistic.class)
                    )
            );
        }

        @Override
        public void processElement(IntStatistic in, Context context, Collector<IntStatistic> out) throws Exception {
            IntStatistic oldAccumulator = accumulator.value();
            if(oldAccumulator == null){
                accumulator.update(new IntStatistic(
                        in.min,
                        in.max,
                        in.sum,
                        1,
                        in.avg
                ));
            }else{
                accumulator.update(new IntStatistic(
                        Math.min(in.min,oldAccumulator.min),
                        Math.max(in.max,oldAccumulator.max),
                        in.sum + oldAccumulator.sum,
                        1 + oldAccumulator.count,
                        (in.sum + oldAccumulator.sum) / (1 + oldAccumulator.count)
                ));
            }
            out.collect(accumulator.value());
        }
    }

    public static class IntSource implements SourceFunction<IntStatistic> {
        private boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<IntStatistic> out) throws Exception {
            int r;
            while (running) {
                r = random.nextInt(1000);
                out.collect(new IntStatistic(
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
            return "（" +
                    "最小值=" + min +
                    ", 最大值=" + max +
                    ", 累加值=" + sum +
                    ", 总条数=" + count +
                    ", 平均数=" + avg + ')';
        }
    }
}
