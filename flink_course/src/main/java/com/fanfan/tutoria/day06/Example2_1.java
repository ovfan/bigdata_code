package com.fanfan.tutoria.day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example2_1
 * @Description: TODO 练习 事件时间 会话窗口
 * @Author: fanfan
 * @DateTime: 2022年08月29日 16时55分
 * @Version: v1.0
 */
public class Example2_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new SourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        // ctx.emitWatermark(new Watermark(Long.MIN_VALUE));
                        ctx.collectWithTimestamp("a", 1000L);
                        ctx.collectWithTimestamp("a", 3000L);

                        // 插入水位线
                        ctx.emitWatermark(new Watermark(10000L));
                        ctx.collectWithTimestamp("a", 13000L);
                        // ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r -> r)
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .process(new ProcessWindowFunction<String, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        out.collect("key为: " + key + "窗口信息: " + context.window().getStart() + " ~ " + context.window().getEnd() + " ，窗口中共有" +
                                elements.spliterator().getExactSizeIfKnown() + "条元素");
                    }
                })
                .print();


        env.execute();
    }
}
