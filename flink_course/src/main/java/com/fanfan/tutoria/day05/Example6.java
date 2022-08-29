package com.fanfan.tutoria.day05;

import com.fanfan.tutoria.utils.ProductViewCountPerWindow;
import com.fanfan.tutoria.utils.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName: Example6
 * @Description: TODO 实现每5分钟统计最近一小时的商品访问次数
 * @Author: fanfan
 * @DateTime: 2022年08月29日 11时46分
 * @Version: v1.0
 */
public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 读取数据源
        env
                .readTextFile("flink_course/src/main/resources/UserBehavior.csv")
                .flatMap(new MyFilterAndMap())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior userBehavior, long l) {
                                return userBehavior.ts;
                            }
                        })
                )
                .keyBy(r -> r.productId)
                .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5)))
                .aggregate(new MyCountAcc(),new MyWindowRes())
                .print();


        env.execute();

    }
    public static class MyWindowRes extends ProcessWindowFunction<Long, ProductViewCountPerWindow,String, TimeWindow>{

        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<ProductViewCountPerWindow> out) throws Exception {
            out.collect(new ProductViewCountPerWindow(
                    // 注意ProcessWindowFunction 中获取窗口的开始时间 和 结束时间 需要使用ctx.window().getStart等来获取
                    key,
                    elements.iterator().next(),
                    context.window().getStart(),
                    context.window().getEnd()
            ));
        }
    }

    public static class MyCountAcc implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long acc) {
            return acc + 1L;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }

    public static class MyFilterAndMap implements FlatMapFunction<String, UserBehavior> {

        @Override
        public void flatMap(String in, Collector<UserBehavior> out) throws Exception {
            String[] array = in.split(",");
            if (array[3].equals("pv")) {
                out.collect(new UserBehavior(
                        array[0], array[1], array[2], array[3], Long.parseLong(array[4]) * 1000L
                ));
            }
        }
    }
}
