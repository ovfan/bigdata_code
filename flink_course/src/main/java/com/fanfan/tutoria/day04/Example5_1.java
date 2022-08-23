package com.fanfan.tutoria.day04;

import com.fanfan.tutoria.utils.ClickEvent;
import com.fanfan.tutoria.utils.ClickSource;
import com.fanfan.tutoria.utils.UserViewCountPerWindow;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example5_1
 * @Description: TODO 计算每个用户每5s 钟的访问次数
 * @Author: fanfan
 * @DateTime: 2022年08月23日 21时40分
 * @Version: v1.0
 */
public class Example5_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<ClickEvent, UserViewCountPerWindow, String, TimeWindow>() {
                    /**
                     *
                     * @param key
                     * @param context
                     * @param elements 迭代器中保存该窗口中所有的元素
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void process(String key, Context context, Iterable<ClickEvent> elements, Collector<UserViewCountPerWindow> out) throws Exception {
                        out.collect(new UserViewCountPerWindow(
                                key,
                                elements.spliterator().getExactSizeIfKnown(),
                                context.window().getStart(),
                                context.window().getEnd()
                        ));
                    }
                })
                .print();

        env.execute();
    }
}
