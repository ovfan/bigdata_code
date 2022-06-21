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
 * @ClassName: Example6_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月21日 17时14分
 * @Version: v1.0
 * TODO 统计每10S用户访问次数
 */
public class Example6_1 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                // 开一个10S中连续滚动的窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new WindowViewCount())
                .print();

        env.execute();
    }
    public static class WindowViewCount extends ProcessWindowFunction<ClickEvent,UserViewCountPerWindow,String,TimeWindow>{
        @Override
        public void process(String key, Context ctx, Iterable<ClickEvent> elements, Collector<UserViewCountPerWindow> out) throws Exception {
            out.collect(new UserViewCountPerWindow(
                    key,
                    elements.spliterator().getExactSizeIfKnown(),//数据条数
                    ctx.window().getStart(),
                    ctx.window().getEnd()
            ));
        }
    }
}
