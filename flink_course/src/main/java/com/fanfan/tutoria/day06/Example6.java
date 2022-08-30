package com.fanfan.tutoria.day06;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import java.time.Duration;

/**
 * @ClassName: Example6
 * @Description: TODO flink处理迟到数据的策略
 * @Author: fanfan
 * @DateTime: 2022年08月29日 20时33分
 * @Version: v1.0
 */
public class Example6 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<String> result = env
                .socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {

                    @Override
                    public Tuple2<String, Long> map(String in) throws Exception {
                        String[] array = in.split(" ");
                        return Tuple2.of(array[0], Long.parseLong(array[1])* 1000L);
                    }
                })
                // 在数据源的下端 插入水位线
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> tuple2, long l) {
                                        // 指定事件时间 字段
                                        return tuple2.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                // 开一个10s 滚动事件时间窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 将迟到且对应窗口已经销毁的数据，发送到侧输出流中
                .sideOutputLateData(
                        // 泛型和窗口中的元素类型一致
                        new OutputTag<Tuple2<String, Long>>("late-event") {
                        }
                )
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        out.collect("key: " + key + "窗口：" + context.window().getStart() + " ~ " + context.window().getEnd() + "里面有" + elements.spliterator().getExactSizeIfKnown() + "条元素");
                    }
                });
        result.print("main");
        result.getSideOutput(new OutputTag<Tuple2<String,Long>>("late-event"){}).print("side");


        env.execute();
    }
}
