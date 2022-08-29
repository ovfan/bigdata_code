package com.fanfan.tutoria.day05;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example7
 * @Description: TODO 自定义水位线发生器
 * @Author: fanfan
 * @DateTime: 2022年08月29日 15时42分
 * @Version: v1.0
 */
public class Example7 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .socketTextStream("hadoop102",9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String in) throws Exception {
                        String[] array = in.split(" ");
                        if(array.length == 2){
                            return  Tuple2.of(array[0],Long.parseLong(array[1]) * 1000L);
                        }
                        return null;
                    }
                })
                // 在map输出的数据流中插入水位线
                // TODO 创建水位线生成器
                .assignTimestampsAndWatermarks(
                        new WatermarkStrategy<Tuple2<String, Long>>() {
                            // 指定事件时间字段
                            @Override
                            public TimestampAssigner<Tuple2<String, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                                return new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> tuple, long l) {
                                        return tuple.f1;
                                    }
                                } ;
                            }

                            // 自定义水位线生成器
                            @Override
                            public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {

                                return new WatermarkGenerator<Tuple2<String, Long>>() {
                                    // 为了防止溢出，maxTs += delay + 1L;
                                    // 最大延迟时间
                                    private long delay = 5000L; // 最大延迟时间
                                    private long maxTs = Long.MIN_VALUE + delay + 1L; // 用来保存观察到的最大事件时间
                                    @Override
                                    public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
                                        // onEvent每来一条数据,调用一次
                                        maxTs = Math.max(maxTs,event.f1);

                                        // 可以针对单个指定的key，发送水位线
                                        if(event.f1.equals("hello")){
                                            output.emitWatermark(new Watermark(12999L));
                                        }
                                    }

                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput output) {
                                        // 周期性调用，默认每隔200ms调用一次
                                        // 向下游发送水位线事件
                                        output.emitWatermark(new Watermark(maxTs - delay -1L));
                                    }
                                };
                            }
                        }
                )
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        out.collect("key：" + key + "，窗口：" + context.window().getStart() + "~" +
                                "" + context.window().getEnd() + "，里面有 " + elements.spliterator().getExactSizeIfKnown() + " 条数据。");
                    }
                })
                .print();

        env.execute();
    }
}
