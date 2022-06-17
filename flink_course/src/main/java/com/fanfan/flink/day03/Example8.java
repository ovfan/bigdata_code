package com.fanfan.flink.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @ClassName: Example8
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月17日 15时11分
 * @Version: v1.0
 * TODO 定时器--KeyProcessFunction<key,in,out>
 * 底层API都是通过process调用
 */
public class Example8 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .socketTextStream("hadoop102", 9999)
                .keyBy(r -> r) //数据按照自身值 作为key分组
                .process(new KeyedProcessFunction<String, String, String>() {
                    // TODO 需求：计算数据的key分组以及数据到来的时间，添加定时器并算出执行的时间
                    @Override
                    public void processElement(String in, Context context, Collector<String> out) throws Exception {
                        long currTs = context.timerService().currentProcessingTime(); // 当前的机器时间
                        long thirtySeconds = currTs + 30 * 1000L;   // 30S后的时间戳
                        long sixtySeconds = currTs + 60 * 1000L;    // 60S后的时间戳

                        // 注册定时器  ctx.timerService().registerProcessingTimeTimer(ts)
                        // 相同的key的数据 会维护一个[定时器列表]--所以 process(KeyedProcessFunction<T..>)是一个有状态的算子
                        context.timerService().registerProcessingTimeTimer(thirtySeconds);
                        context.timerService().registerProcessingTimeTimer(sixtySeconds);

                        out.collect(
                                "key: " + context.getCurrentKey() + "数据为：" + in + " ，数据到来的时间为：" + new Timestamp(currTs) + " ,"
                                        + "注册的第一个定时器的时间为:" + new Timestamp(thirtySeconds) + "注册的第二个定时器时间为: " + new Timestamp(sixtySeconds)
                        );
                    }

                    /**
                     * 定时器的处理逻辑
                     * @param timerTs 输入进来的ts --> thirtySeconds / sixtySeconds
                     * @param ctx 上下文对象
                     * @param out  输出的内容
                     * @throws Exception
                     */
                    @Override
                    public void onTimer(long timerTs, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("key为" + ctx.getCurrentKey() + "的定时器触发了 " + "定时器的时间戳为：" + new Timestamp(timerTs) + " 真正执行的时间为：" + new Timestamp(ctx.timerService().currentProcessingTime()));
                    }
                })
                .print()
                .setParallelism(1);


        env.execute();
    }
}
