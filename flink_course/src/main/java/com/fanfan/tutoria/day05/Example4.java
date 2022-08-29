package com.fanfan.tutoria.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


/**
 * @ClassName: Example4
 * @Description: TODO 水位线测试
 * assigner 分配
 * @Author: fanfan
 * @DateTime: 2022年08月29日 09时58分
 * @Version: v1.0
 */
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置插入水位线的时间间隔
//        env.getConfig().setAutoWatermarkInterval(60 * 1000L);

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile("flink_course/src/main/resources/my.properties");
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        env
                .socketTextStream(host, port)
                .flatMap(new MyFlatMap())
                // 在map输出的数据流中插入水位线事件，默认是200ms插入一次
                .assignTimestampsAndWatermarks(
                        // 设置水位线的最大延迟时间 （不要忘记方法返回值类型位Tuple2<String,Long>）
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                // 指定事件时间字段
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> elements, long l) {
                                        return elements.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                // 开一个 10s 的滚动事件时间窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        out.collect("key为:" + key + " ,窗口为: " + context.window().getStart() + "~" + context.window().getEnd() + " ,窗口中共有" +
                                elements.spliterator().getExactSizeIfKnown() + "个元素"
                        );
                    }
                }).print();


        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String, Long>> {

        @Override
        public void flatMap(String in, Collector<Tuple2<String, Long>> out) throws Exception {
            String[] words = in.split("\\s+");
            // 数组中存储的类型都是相同的
            out.collect(Tuple2.of(words[0], Long.parseLong(words[1]) * 1000L));
        }
    }
}
