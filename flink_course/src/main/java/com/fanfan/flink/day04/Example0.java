package com.fanfan.flink.day04;

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
 * @ClassName: Example0
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月21日 09时23分
 * @Version: v1.0
 * TODO 需求:使用值状态变量完成统计值案例
 */
public class Example0 {
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

    // 统计值的实现类
    public static class MyStatistic extends KeyedProcessFunction<String, Integer, IntStatistic> {
        // 声明值状态变量保存 统计值累加器
        private ValueState<IntStatistic> acc;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化值状态变量
            acc = getRuntimeContext().getState(
                    new ValueStateDescriptor<IntStatistic>(
                            "acc",//状态描述符
                            Types.POJO(IntStatistic.class) //ACC的累加器是实现类
                    )
            );
        }

        @Override
        public void processElement(Integer in, Context ctx, Collector<IntStatistic> out) throws Exception {
            // 判断累加器值是否为空，为空则初始化
            if (acc.value() == null) {
                acc.update(new IntStatistic(
                        in,
                        in,
                        in,
                        1,
                        in
                ));
            } else {
                // TODO 如果有累加器获取累加器值与 输入进来的数据进行计算产生新的累加器
                IntStatistic oldAcc = acc.value();
                acc.update(new IntStatistic(
                        Math.min(in, oldAcc.min),
                        Math.max(in, oldAcc.max),
                        in + oldAcc.sum,
                        1 + oldAcc.count,
                        (in + oldAcc.sum) / (1 + oldAcc.count)
                ));
            }
            // 向下游发送数据
            out.collect(acc.value());
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
