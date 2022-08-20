package com.fanfan.tutoria.day03;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @ClassName: Example8
 * @Description: TODO 使用KeyedProcessFunction实现定时器功能
 * @Author: fanfan
 * @DateTime: 2022年08月20日 14时44分
 * @Version: v1.0
 */
public class Example8 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile("flink_course/src/main/resources/info.properties");
        String host = parameterTool.get("linuxhost");
        env
                .socketTextStream(host, 9999)
                .keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String in) throws Exception {
                        return "string";
                    }
                })
                .process(new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        // 1. 获取当前系统时间
                        long currentTs = ctx.timerService().currentProcessingTime();
                        // 2. 定义 30s 60s 之后的时间戳
                        long thirtySecondsLater = currentTs + 30 * 1000L;
                        long sixtySecondsLater = currentTs + 60 * 1000L;
                        // 3. 注册定时器
                        ctx.timerService().registerProcessingTimeTimer(thirtySecondsLater);
                        ctx.timerService().registerProcessingTimeTimer(sixtySecondsLater);
                        // 4. 打印
                        out.collect("输入数据: " + in + ",key是: " + ctx.getCurrentKey() + "，数据到达的时间为: "
                                        + new Timestamp(currentTs) + ", 注册了定时器: " + new Timestamp(thirtySecondsLater) + ", " + new Timestamp(sixtySecondsLater)
                                );
                    }

                    // ts 代表定时器的时间戳
                    @Override
                    public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("key是" + ctx.getCurrentKey() + ",定时器" + new Timestamp(ts) + "定时器真正执行的机器时间为:" + new Timestamp(ctx.timerService().currentProcessingTime()));
                    }
                })
                .print();

        env.execute();
    }
}
