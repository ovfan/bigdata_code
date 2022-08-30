package com.fanfan.tutoria.day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example1_1
 * @Description: TODO 练习处理时间语义的 会话窗口
 * @Author: fanfan
 * @DateTime: 2022年08月29日 16时47分
 * @Version: v1.0
 */
public class Example1_1 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new SourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        ctx.collect("a");
                        Thread.sleep(1000L);
                        ctx.collect("a");
                        Thread.sleep(10000L);
                        ctx.collect("a");
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r -> r)
                // 开一个 处理时间的 会话窗口
                // 超时时间 设置为 5s
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .process(new ProcessWindowFunction<String, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        out.collect("key为: " + key + "窗口信息:" + context.window().getStart() + " ~ " + context.window().getEnd() + "窗口中共有"
                         + elements.spliterator().getExactSizeIfKnown() + "条元素"
                        );
                    }
                })
                .print();

        env.execute();
    }
}
