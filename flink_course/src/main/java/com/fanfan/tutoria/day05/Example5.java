package com.fanfan.tutoria.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName: Example5
 * @Description: 测试水位线 插入的时机
 * @Author: fanfan
 * @DateTime: 2022年08月29日 11时24分
 * @Version: v1.0
 */
public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String in) throws Exception {
                        String[] array = in.split(" ");

                        return Tuple2.of(array[0], Long.parseLong(array[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> tuple, long l) {
                                        return tuple.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                // KeyedProcessFunction 第一个参数是key的类型，第二个参数是输入的类型，第三个参数是输出数据的类型
                // 注意，keyedprocessFunction 不要与 window() 开窗函数混用
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> in, Context ctx, Collector<String> out) throws Exception {
                        ctx.timerService().registerEventTimeTimer(in.f1 + 9000L);
                        out.collect("key为: " + ctx.getCurrentKey() + " ，当前process的水位线: " + ctx.timerService().currentWatermark()
                        + " ,时间戳是: " + in.f1 + " ,注册的定时器时间戳是: " + (in.f1 + 9000L)
                        );
                    }

                    @Override
                    public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("key为: " + ctx.getCurrentKey() + " ，当前process的水位线: " + ctx.timerService().currentWatermark()
                                +  " ,注册的定时器时间戳是: " + ts
                        );
                    }
                })
                .print();

        env.execute();
    }
}
