package com.fanfan.tutoria.day07;

import com.fanfan.tutoria.revise.PVCount;
import com.fanfan.tutoria.utils.ProductViewCountPerWindow;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;

/**
 * @ClassName: Example2
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月26日 09时18分
 * @Version: v1.0
 */
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop103:9092");


        env
                .addSource(new FlinkKafkaConsumer<String>(
                        "topic-userbehavior-0321",
                        new SimpleStringSchema(),
                        properties
                ))

//                .flatMap(new FlatMapFunction<String, PVCount.UserBehavior>() {
//
//                    @Override
//                    public void flatMap(String in, Collector<PVCount.UserBehavior> out) throws Exception {
//                        String[] array = in.split(",");
//                        PVCount.UserBehavior userBehavior = new PVCount.UserBehavior(
//                                array[0], array[1], array[2], array[3],
//                                Long.parseLong(array[4]) * 1000L
//                        );
//                        if(userBehavior.type.equals("pv")){
//                            out.collect(userBehavior);
//                        }
//                    }
//                })
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<PVCount.UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
//                    .withTimestampAssigner(new SerializableTimestampAssigner<PVCount.UserBehavior>() {
//                        @Override
//                        public long extractTimestamp(PVCount.UserBehavior userBehavior, long l) {
//                            return userBehavior.ts;
//                        }
//                    })
//                )
//                .keyBy( r -> r.productID)
//                .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5)))
//                .aggregate(new AggregateFunction<PVCount.UserBehavior, Long, Long>() {
//                    @Override
//                    public Long createAccumulator() {
//                        return 0L;
//                    }
//
//                    @Override
//                    public Long add(PVCount.UserBehavior userBehavior, Long acc) {
//                        return acc + 1L;
//                    }
//
//                    @Override
//                    public Long getResult(Long acc) {
//                        return acc;
//                    }
//
//                    @Override
//                    public Long merge(Long aLong, Long acc1) {
//                        return null;
//                    }
//                }, new ProcessWindowFunction<Long, ProductViewCountPerWindow, String, TimeWindow>() {
//                    @Override
//                    public void process(String key, Context context, Iterable<Long> elements, Collector<ProductViewCountPerWindow> out) throws Exception {
//                        out.collect(new ProductViewCountPerWindow(
//                                key,
//                                elements.iterator().next(),
//                                context.window().getStart(),
//                                context.window().getEnd()
//                        ));
//                    }
//                })
//                .keyBy(new KeySelector<ProductViewCountPerWindow, Tuple2<Long,Long>>() {
//                    @Override
//                    public Tuple2<Long, Long> getKey(ProductViewCountPerWindow in) throws Exception {
//                        return Tuple2.of(in.windowStartTime, in.windowEndTime);
//                    }
//                })
//                .process(new TopN(3))
                .print();

        env.execute();
    }

    public static class TopN extends KeyedProcessFunction<Tuple2<Long, Long>, ProductViewCountPerWindow, String> {

        private int n;

        public TopN(int n) {
            this.n = n;
        }

        private ListState<ProductViewCountPerWindow> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ProductViewCountPerWindow>(
                            "list-state",
                            Types.POJO(ProductViewCountPerWindow.class)
                    )
            );
        }

        @Override
        public void processElement(ProductViewCountPerWindow in, Context ctx, Collector<String> out) throws Exception {
            listState.add(in);

            // 保证所有的属于in.windowStartTime ~ in.windowEndTime的ProductViewCountPerWindow都添加到listState中
            ctx.timerService().registerEventTimeTimer(in.windowEndTime + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<ProductViewCountPerWindow> arrayList = new ArrayList<>();
            for (ProductViewCountPerWindow p : listState.get()) arrayList.add(p);
            // 手动gc
            listState.clear();

            arrayList.sort(new Comparator<ProductViewCountPerWindow>() {
                @Override
                public int compare(ProductViewCountPerWindow p1, ProductViewCountPerWindow p2) {
                    return (int) (p2.count - p1.count);
                }
            });

            StringBuilder result = new StringBuilder();
            result.append("============" + new Timestamp(ctx.getCurrentKey().f0) + "~" + new Timestamp(ctx.getCurrentKey().f1) + "============\n");
            for (int i = 0; i < n; i++) {
                ProductViewCountPerWindow tmp = arrayList.get(i);
                result.append("第" + (i + 1) + "名的商品ID：" + tmp.productId + ",浏览次数：" + tmp.count + "\n");
            }
            result.append("===================================================================\n");
            out.collect(result.toString());
        }
    }
}
