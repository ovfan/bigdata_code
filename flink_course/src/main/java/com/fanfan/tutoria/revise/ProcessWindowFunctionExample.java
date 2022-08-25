package com.fanfan.tutoria.revise;

import com.fanfan.tutoria.utils.ClickEvent;
import com.fanfan.tutoria.utils.ClickSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @ClassName: ProcessWindowFunctionExample
 * @Description: TODO 计算每个用户在每5s的访问次数
 * @Author: fanfan
 * @DateTime: 2022年08月25日 17时56分
 * @Version: v1.0
 */
public class ProcessWindowFunctionExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new WindowResult())
                .print();

        env.execute();
    }
    public static class UserViewCountPerWindow{
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
    public static class WindowResult extends ProcessWindowFunction<ClickEvent,UserViewCountPerWindow,String, TimeWindow>{
        @Override
        public void process(String key, Context context, Iterable<ClickEvent> in, Collector<UserViewCountPerWindow> out) throws Exception {

            long count = in.spliterator().getExactSizeIfKnown();
            out.collect(new UserViewCountPerWindow(
                    key,
                    count,
                    context.window().getStart(),
                    context.window().getEnd()
            ));
        }
    }
}
