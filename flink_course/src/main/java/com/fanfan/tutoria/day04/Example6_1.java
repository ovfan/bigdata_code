package com.fanfan.tutoria.day04;

import com.fanfan.tutoria.utils.ClickEvent;
import com.fanfan.tutoria.utils.ClickSource;
import com.fanfan.tutoria.utils.UserViewCountPerWindow;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example6_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月23日 22时03分
 * @Version: v1.0
 */
public class Example6_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(new KeySelector<ClickEvent, String>() {
                    @Override
                    public String getKey(ClickEvent in) throws Exception {
                        return in.username;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<ClickEvent, Long, Long>() {
                    /**
                     * 累加器的初始值
                     * @return
                     */
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    // 输入数据和累加器的聚合规则
                    @Override
                    public Long add(ClickEvent in, Long accumulator) {
                        return accumulator + 1L;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long aLong, Long acc1) {
                        return null;
                    }
                }, new ProcessWindowFunction<Long, UserViewCountPerWindow, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Long> elements, Collector<UserViewCountPerWindow> out) throws Exception {
                        out.collect(new UserViewCountPerWindow(
                                key,
                                elements.iterator().next(),
                                context.window().getStart(),
                                context.window().getEnd()
                        ));
                    }
                }).print();


        env.execute();
    }
}
