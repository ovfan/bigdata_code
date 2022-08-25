package com.fanfan.tutoria.revise;

import com.fanfan.tutoria.utils.ClickEvent;
import com.fanfan.tutoria.utils.ClickSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @ClassName: AggregateFunctionExample
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月25日 18时14分
 * @Version: v1.0
 */
public class AggregateFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new MyCountAgg(), new NyWindowResult())
                .print();

        env.execute();
    }

    public static class MyCountAgg implements AggregateFunction<ClickEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ClickEvent in, Long acc) {
            return acc + 1L;
        }

        @Override
        public Long getResult(Long result) {
            return result;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }

    }

    public static class NyWindowResult extends ProcessWindowFunction<Long, UserViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<UserViewCountPerWindow> out) throws Exception {
            out.collect(new UserViewCountPerWindow(
                    key,
                    elements.iterator().next(),
                    context.window().getStart(),
                    context.window().getEnd()
            ));
        }
    }

    public static class UserViewCountPerWindow {
        public String username;
        public Long count;
        public Long windowStartTime;
        public Long windowEndTime;

        public UserViewCountPerWindow(String username, Long count, Long windowStartTime, Long windowEndTime) {
            this.username = username;
            this.count = count;
            this.windowStartTime = windowStartTime;
            this.windowEndTime = windowEndTime;
        }

        public UserViewCountPerWindow() {
        }

        @Override
        public String toString() {
            return "UserViewCountPerWindow{" +
                    "username='" + username + '\'' +
                    ", count=" + count +
                    ", windowStartTime=" + new Timestamp(windowStartTime) +
                    ", windowEndTime=" + new Timestamp(windowEndTime) +
                    '}';
        }
    }

}
