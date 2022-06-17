package com.fanfan.flink.day03;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @ClassName: Example9
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月17日 16时28分
 * @Version: v1.0
 * TODO 自定义Int数据源，利用值状态变量求出(最大值，最小值，累加值，出现条数，平均值)
 */
public class Example9 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new IntSource())
                .keyBy(r -> "int")
                .process(new MyStatistic())
                .print();

        env.execute();
    }

    public static class MyStatistic extends KeyedProcessFunction<String, Integer, IntStatistic> {
        // 声明一个值状态变量
        private ValueState<IntStatistic> accumulator;

        /**
         * 初始化值状态变量
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            accumulator = getRuntimeContext().getState(new ValueStateDescriptor<IntStatistic>(
                    // 针对每个状态变量取一个唯一的名字
                    "accumulator",
                    Types.POJO(IntStatistic.class)
            ));
        }

        @Override
        public void processElement(Integer in, Context context, Collector<IntStatistic> out) throws Exception {
            if (accumulator.value() == null) {
                //初始化值状态变量
                // accumulator.value() == null说明到达的数据是数据流的第一条数据
                // accumulator.value()获取的是输入数据in的key所对应的值状态变量中的值
                accumulator.update(new IntStatistic(in, in, in, 1, in));

            } else { //状态变量有初始值，输出计算结果
                IntStatistic oldAccumulator = accumulator.value();
                IntStatistic newAccumulator = new IntStatistic(
                        Math.min(in, oldAccumulator.min),
                        Math.max(in, oldAccumulator.max),
                        in + oldAccumulator.sum,
                        1 + oldAccumulator.count,
                        (in + oldAccumulator.sum) / (1 + oldAccumulator.count)
                );
                accumulator.update(newAccumulator);
            }
            out.collect(accumulator.value());
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
            return "(最小值：" + min + " ,最大值：" + max + " ,总和：" + sum + " ,总条数：" + count + " ,平均值" + avg + ")";
        }
    }

    // 数据源
    public static class IntSource implements SourceFunction<Integer> {
        private boolean running = true;       // 持续生产数据
        private Random random = new Random(); // random对象用于生成随机数

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                ctx.collect(random.nextInt(1000));
                Thread.sleep(1000L); // 1s 生成一个随机数字
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
