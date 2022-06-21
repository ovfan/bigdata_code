package com.fanfan.flink.day04;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Random;

/**
 * @ClassName: Example_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月21日 09时56分
 * @Version: v1.0
 * TODO 计算累加值，每隔10S打印一次输出结果
 *
 * TODO : 整条流的数据存下来，存到列表状态变量中，用ArrayList flink底层序列化时性能非常差
 */
public class Example_1 {
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

    // TODO 初始化值状态变量与 完成定时器功能
    public static class MyStatistic extends KeyedProcessFunction<String, Integer, IntStatistic> {
        private ValueState<IntStatistic> acc;
        private ValueState<Integer> flag;

        @Override
        public void open(Configuration parameters) throws Exception {
            //TODO 声明值状态变量
            acc = getRuntimeContext().getState(
                    new ValueStateDescriptor<IntStatistic>(
                            "ACC",
                            Types.POJO(IntStatistic.class)
                    )
            );
            flag = getRuntimeContext().getState(
                    new ValueStateDescriptor<Integer>(
                            "flag",
                            Types.INT
                    )
            );
        }

        @Override
        public void processElement(Integer in, Context context, Collector<IntStatistic> out) throws Exception {
            IntStatistic oldAcc = acc.value();
            if (oldAcc == null) {
                acc.update(new IntStatistic(
                        in,
                        in,
                        in,
                        1,
                        in
                ));
            } else {
                acc.update(new IntStatistic(
                        Math.min(in, oldAcc.min),
                        Math.max(in, oldAcc.max),
                        in + oldAcc.sum,
                        1 + oldAcc.count,
                        (in + oldAcc.sum) / (1 + oldAcc.count)
                ));
            }

            // 初始化定时器
            if (flag.value() == null) { //此时还没有定时器，则注册一个定时器
                // 获取当前时间
                long currTs = context.timerService().currentProcessingTime();
                context.timerService().registerProcessingTimeTimer(currTs + 10 * 1000L); //注册一个十秒后的定时器
                flag.update(1);
            }
        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx, Collector<IntStatistic> out) throws Exception {
            // 在定时器中 向下游发送数据
            out.collect(acc.value());

            //清空当前key的定时器
            flag.clear();
        }

        @Override
        public void close() throws Exception {

        }
    }

    // 数据源
    public static class IntSource implements SourceFunction<Integer> {
        private boolean running = true;
        private Random random = new Random();

        // 生成数据，一秒生产一条
        @Override
        public void run(SourceContext<Integer> out) throws Exception {
            int randNUm;
            while (running) {
                randNUm = random.nextInt(1000);
                out.collect(randNUm);

                // 一秒发送一次
                Thread.sleep(1000L);
            }
        }

        // 取消生成数据
        @Override
        public void cancel() {

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
            return "统计值{" +
                    "min=" + min +
                    ", max=" + max +
                    ", sum=" + sum +
                    ", count=" + count +
                    ", avg=" + avg +
                    '}';
        }
    }
}
