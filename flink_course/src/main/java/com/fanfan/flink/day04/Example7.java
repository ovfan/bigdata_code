package com.fanfan.flink.day04;

import com.fanfan.flink.utils.ClickEvent;
import com.fanfan.flink.utils.ClickSource;
import com.fanfan.flink.utils.UserViewCountPerWindow;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example7
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月21日 17时30分
 * @Version: v1.0
 */
public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                // 开一个长度为10S的滚动窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new CountAgg(), new WindowResult())
                .print();
        // 当AggregateFunction和ProcessWindowFunction结合使用时，
        // 需要调用aggregate方法
        env.execute();
    }

    public static class CountAgg implements AggregateFunction<ClickEvent, Long, Long> {
        // 1.创建窗口时，初始化一个累加器
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        // 2.每来一条数据，累加器加一
        @Override
        public Long add(ClickEvent in, Long acc) {
            return acc + 1L;
        }
        //3.窗口闭合时，触发调用
        //  将返回值发送出去

        @Override
        public Long getResult(Long accResult) {
            return accResult;
        }

        // 4. merge

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long, UserViewCountPerWindow, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<UserViewCountPerWindow> out) throws Exception {
            // elements中只有一个最终结果：
            out.collect(new UserViewCountPerWindow(
                    key,
                    // 将迭代器中唯一的元素取出
                    elements.iterator().next(),
                    context.window().getStart(),
                    context.window().getEnd()
            ));
        }
    }
}
