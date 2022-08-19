package com.fanfan.flink.day03_2;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @ClassName: Example7
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月16日 08时54分
 * @Version: v1.0
 */
public class Example7 {
    // TODO 练习KeyedProcessFunction -- 定时器练习
    //  添加定时器并算出执行的时间
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .socketTextStream("hadoop102",9999)
                .keyBy(r -> r)
                .process(new KeyedProcessFunction<String,String,String>(){
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        // 获取当前系统时间
                        long ts = ctx.timerService().currentProcessingTime();
                        long thirtySeconds = ts + 3000L;
                        long SixtySeconds = ts + 6000L;

                        // 注册定时器
                        ctx.timerService().registerProcessingTimeTimer(thirtySeconds);
                        ctx.timerService().registerProcessingTimeTimer(SixtySeconds);
                        out.collect("数据: " + in + ", key为: " + "注册的定时器为" + new Timestamp(thirtySeconds));
                        out.collect("数据: " + in + ", key为: " + "注册的定时器为" + new Timestamp(SixtySeconds));
                    }

                    @Override
                    public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // ts为输入的定时器时间
                        System.out.println(new Timestamp(ts) + "定时器触发了");
                    }
                })
                .print();

        env.execute();
    }
}
