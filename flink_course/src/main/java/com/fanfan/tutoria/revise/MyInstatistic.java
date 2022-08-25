package com.fanfan.tutoria.revise;

import com.fanfan.flink.day04.Example2_1;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: MyInstatistic
 * @Description: TODO 适用值状态变量 + 累加器 实现整数流的限流统计
 * @Author: fanfan
 * @DateTime: 2022年08月25日 12时26分
 * @Version: v1.0
 */
public class MyInstatistic {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new Example2_1.IntSource())
                .keyBy(r -> "int")
                .process(new MyCount())
                .print();


        env.execute();

    }

    public static class MyCount extends KeyedProcessFunction<String, Integer, String> {
        // 1. flink底层API 都是富函数
        // 定义值状态变量
        private ValueState<Intstatistic> acc;
        private ValueState<Boolean> flag;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化值状态变量
            acc = getRuntimeContext().getState(new ValueStateDescriptor<Intstatistic>(
                    "acc",
                    Types.POJO(Intstatistic.class)
            ));

            flag = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>(
                    "flag",
                    Types.BOOLEAN
            ));
        }

        @Override
        public void processElement(Integer in, Context ctx, Collector<String> out) throws Exception {
            Intstatistic oldAcc = acc.value();
            if (oldAcc == null) {
                acc.update(new Intstatistic(in, in, in, 1, in));
            } else {
                acc.update(new Intstatistic(
                        Math.min(in, oldAcc.min),
                        Math.max(in, oldAcc.max),
                        in + oldAcc.sum,
                        1 + oldAcc.count,
                        (in + oldAcc.sum) / (1 + oldAcc.count)
                ));
            }
            if (flag.value() == null) {
                // 获取当前机器时间
                long currTs = ctx.timerService().currentProcessingTime();
                // 注册10s以后的定时器
                ctx.timerService().registerProcessingTimeTimer(currTs + 10 * 1000L);
                flag.update(true);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(acc.value().toString());
            flag.update(null);
        }
    }
}
