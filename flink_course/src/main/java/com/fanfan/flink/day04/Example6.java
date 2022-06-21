package com.fanfan.flink.day04;

import com.fanfan.flink.utils.ClickEvent;
import com.fanfan.flink.utils.ClickSource;
import com.fanfan.flink.utils.UserViewCountPerWindow;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example6
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月21日 16时45分
 * @Version: v1.0
 * TODO 熟悉窗口的使用
 */
public class Example6 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                // 开一个连续10S的滚动窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new WindowResult())
                .print();

        env.execute();
    }
    public static class WindowResult extends ProcessWindowFunction<ClickEvent, UserViewCountPerWindow,String, TimeWindow> {
        // key为 分组字段的key 即用户名~~
        @Override
        public void process(String key, Context ctx, Iterable<ClickEvent> elements, Collector<UserViewCountPerWindow> out) throws Exception {
            // Iterable<ClickEvent> elements 包含了属于窗口的所有元素
            // elements.spliterator().getExactSizeIfKnown() 窗口中元素的个数
            out.collect(new UserViewCountPerWindow(
                    key,
                    elements.spliterator().getExactSizeIfKnown(),
                    ctx.window().getStart(), //开始时间戳
                    ctx.window().getEnd()
            ));
        }
    }
}
