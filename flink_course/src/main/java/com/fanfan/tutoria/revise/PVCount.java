package com.fanfan.tutoria.revise;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @ClassName: PVCount
 * @Description: TODO 每隔五分钟，统计每个商品在过去一小时的访问次数
 * @Author: fanfan
 * @DateTime: 2022年08月25日 23时11分
 * @Version: v1.0
 */
public class PVCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.读取数据
        env
                .readTextFile("flink_course/src/main/resources/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String in) throws Exception {
                        String[] array = in.split(",");
                        return new UserBehavior(
                                array[0], array[1], array[2], array[3], Long.parseLong(array[4]) * 1000L
                        );
                    }
                })
                .filter(r -> r.type.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior userBehavior, long l) {
                                return userBehavior.ts;
                            }
                        })
                )
                .keyBy(r -> r.productID)
                // 窗口长度一小时，滑动距离5分钟
                .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5)))
                .aggregate(new AggregateFunction<UserBehavior, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(UserBehavior userBehavior, Long acc) {
                        return acc + 1;
                    }

                    @Override
                    public Long getResult(Long acc) {
                        return acc;
                    }

                    @Override
                    public Long merge(Long aLong, Long acc1) {
                        return null;
                    }
                }, new ProcessWindowFunction<Long, ProductViewCountPerWindow, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Long> elements, Collector<ProductViewCountPerWindow> out) throws Exception {
                        out.collect(
                                new ProductViewCountPerWindow(
                                        key,
                                        elements.iterator().next(),
                                        context.window().getStart(),
                                        context.window().getEnd()
                                )
                        );
                    }
                })
                .print();


        env.execute();
    }
    public static class ProductViewCountPerWindow {
        public String productID;
        public Long count;
        public Long windowStartTime;
        public Long windowEndTime;

        public ProductViewCountPerWindow(String productID, Long count, Long windowStartTime, Long windowEndTime) {
            this.productID = productID;
            this.count = count;
            this.windowStartTime = windowStartTime;
            this.windowEndTime = windowEndTime;
        }

        public ProductViewCountPerWindow() {
        }

        @Override
        public String toString() {
            return "productViewCountPerWindow{" +
                    "productID='" + productID + '\'' +
                    ", count=" + count +
                    ", windowStartTime=" + new Timestamp(windowStartTime) +
                    ", windowEndTime=" + new Timestamp(windowEndTime) +
                    '}';
        }
    }

    public static class UserBehavior {
        public String userID;
        public String productID;
        public String CategoryID;
        public String type;
        public Long ts;

        public UserBehavior(String userID, String productID, String categoryID, String type, Long ts) {
            this.userID = userID;
            this.productID = productID;
            CategoryID = categoryID;
            this.type = type;
            this.ts = ts;
        }

        public UserBehavior() {
        }

    }
}
