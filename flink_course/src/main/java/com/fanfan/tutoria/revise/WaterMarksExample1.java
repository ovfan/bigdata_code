package com.fanfan.tutoria.revise;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
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
 * @ClassName: WaterMarksExample1
 * @Description: 测试水位线 程序1
 * @Author: fanfan
 * @DateTime: 2022年08月25日 21时17分
 * @Version: v1.0
 */
public class WaterMarksExample1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile("flink_course/src/main/resources/my.properties");
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        env.setParallelism(1);
        env
                .socketTextStream(host, port)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String in) throws Exception {
                        String[] array = in.split("\\s+");
                        long eventTs = Long.parseLong(array[1]);
                        return Tuple2.of(array[0], eventTs * 1000L);
                    }
                })
                // 在map输出的数据流中插入水位线事件
                .assignTimestampsAndWatermarks(
                        // 插入水位线事件，一定要注意水位线策略 这个方法的泛型，要加上
                        // 将最大延迟时间设置为 5s
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
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        out.collect("key为: " + key + "窗口: " + context.window().getStart() + " ~ " + context.window().getEnd() + " ,里面有" + elements.spliterator().getExactSizeIfKnown() + "条数据");
                    }
                })
                .print();

        env.execute();
    }
}
