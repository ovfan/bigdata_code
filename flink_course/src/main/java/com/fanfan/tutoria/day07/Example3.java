package com.fanfan.tutoria.day07;

import com.fanfan.tutoria.revise.PVCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.base.Charsets;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Properties;

/**
 * @ClassName: Example3
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月26日 14时01分
 * @Version: v1.0
 */
public class Example3 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.put("bootstrap.servers","hadoop102:9092");
        env.setParallelism(1);
        env
                .readTextFile("flink_course/src/main/resources/UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, PVCount.UserBehavior>() {


                    @Override
                    public void flatMap(String in, Collector<PVCount.UserBehavior> out) throws Exception {
                        String[] array = in.split(",");
                        PVCount.UserBehavior userBehavior = new PVCount.UserBehavior(
                                array[0],array[1],array[2],array[3],Long.parseLong(array[4]) * 1000L
                        );
                        if(userBehavior.type.equals("pv")){
                            out.collect(userBehavior);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PVCount.UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<PVCount.UserBehavior>() {
                            @Override
                            public long extractTimestamp(PVCount.UserBehavior userBehavior, long l) {
                                return userBehavior.ts;
                            }
                        })
                )
                .keyBy(r -> "userbehavior")
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(
                        new AggregateFunction<PVCount.UserBehavior, Tuple2<BloomFilter<String>, Long>, Long>() {

                            @Override
                            public Tuple2<BloomFilter<String>, Long> createAccumulator() {
                                return Tuple2.of(
                                        BloomFilter.create(
                                                // 去重的数据类型
                                                Funnels.stringFunnel(Charsets.UTF_8),
                                                // 预估的去重数量
                                                20000,
                                                // 误判率
                                                0.001
                                        ),
                                        // 计数器
                                        0L
                                );
                            }

                            @Override
                            public Tuple2<BloomFilter<String>, Long> add(PVCount.UserBehavior value, Tuple2<BloomFilter<String>, Long> accumulator) {
                                // 如果用户之前一定没来过，计数器加一
                                if (!accumulator.f0.mightContain(value.userID)) {
                                    accumulator.f0.put(value.userID);
                                    accumulator.f1 += 1L;
                                }
                                return accumulator;
                            }

                            @Override
                            public Long getResult(Tuple2<BloomFilter<String>, Long> acc) {
                                return acc.f1;
                            }

                            @Override
                            public Tuple2<BloomFilter<String>, Long> merge(Tuple2<BloomFilter<String>, Long> bloomFilterLongTuple2, Tuple2<BloomFilter<String>, Long> acc1) {
                                return null;
                            }
                        },
                        new ProcessWindowFunction<Long, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
                                out.collect("窗口" + new Timestamp(context.window().getStart()) + "~" +
                                        "" + new Timestamp(context.window().getEnd()) + "的uv是：" +
                                        "" + elements.iterator().next());
                            }
                        }

                )
                .print();


        env.execute();
    }
}
