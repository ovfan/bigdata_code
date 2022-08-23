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
 * @ClassName: Example5
 * @Description: TODO 计算每个用户在每5s钟的访问次数
 * @Author: fanfan
 * @DateTime: 2022年08月21日 16时45分
 * @Version: v1.0
 */
public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                // 5秒钟的滚动处理时间窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new WindowResult())
                .print();
        env.execute();
    }

    // 窗口的类型 --- 时间窗口
    public static class WindowResult extends ProcessWindowFunction<ClickEvent, UserViewCountPerWindow, String, TimeWindow> {

        /**
         * @param key
         * @param context
         * @param elements 迭代器 包含了属于窗口的所有元素
         * @param out
         * @throws Exception
         */
        @Override
        public void process(String key, Context context, Iterable<ClickEvent> elements, Collector<UserViewCountPerWindow> out) throws Exception {
            out.collect(new UserViewCountPerWindow(
                    key,
                    elements.spliterator().getExactSizeIfKnown(), // 获取迭代器中的元素数量
                    context.window().getStart(), // 窗口开始时间
                    context.window().getEnd()   // 窗口结束时间
            ));
        }
    }

}
