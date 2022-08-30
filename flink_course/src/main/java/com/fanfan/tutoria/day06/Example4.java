package com.fanfan.tutoria.day06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName: Example4
 * @Description: TODO 测试分流 水位线传播机制
 * @Author: fanfan
 * @DateTime: 2022年08月29日 19时31分
 * @Version: v1.0
 */
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String in) throws Exception {
                        String[] array = in.split(" ");
                        return Tuple2.of(array[0], Long.parseLong(array[1]) * 1000L);
                    }
                })
                .setParallelism(1)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> tuple, long l) {
                                return tuple.f1;
                            }
                        })
                )
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        out.collect("key：" + key + ",窗口信息: " + context.window().getStart() + " ~ " + context.window().getEnd() + " ,当前水位线为:"
                                + context.currentWatermark() + "窗口中有" + elements.spliterator().getExactSizeIfKnown() + "条元素");
                    }
                })
                .setParallelism(4)
                .print()
                .setParallelism(4);


        env.execute();
    }
}
