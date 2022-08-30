package com.fanfan.tutoria.day06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName: Example5
 * @Description: TODO 测试合流 时候的水位线传播机制
 * @Author: fanfan
 * @DateTime: 2022年08月29日 19时43分
 * @Version: v1.0
 */
public class Example5 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置两条 socket文本流
        SingleOutputStreamOperator<Tuple2<String, Long>> stream1 = env
                .socketTextStream("hadoop102", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String in, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] array = in.split(" ");
                        out.collect(Tuple2.of(array[0], Long.parseLong(array[1]) * 1000L));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> tuple2, long l) {
                                        return tuple2.f1;
                                    }
                                })
                );


        SingleOutputStreamOperator<Tuple2<String, Long>> stream2 = env
                .socketTextStream("hadoop102", 9998)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String in, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] array = in.split(" ");
                        out.collect(Tuple2.of(array[0], Long.parseLong(array[1]) * 1000L));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> tuple2, long l) {
                                        return tuple2.f1;
                                    }
                                })
                );

        // 两条流合流
        stream1
                .union(stream2)
                .process(new ProcessFunction<Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> in, Context ctx, Collector<String> out) throws Exception {
                        out.collect("输入数据：" + in + "，当前process的水位线是：" + ctx.timerService().currentWatermark());
                    }
                })
                .print();


        env.execute();
    }
}
