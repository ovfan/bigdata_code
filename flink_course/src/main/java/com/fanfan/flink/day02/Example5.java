package com.fanfan.flink.day02;

import com.fanfan.flink.utils.ClickEvent;
import com.fanfan.flink.utils.ClickSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example5
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月16日 19时08分
 * @Version: v1.0
 * TODO 基本转换算子 flatMap、map、filter
 */
public class Example5 {
    public static void main(String[] args) throws Exception {
        // TODO 1.使用map从数据源获取数据，过滤出用户的姓名
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .map(r -> r.username)
                .print("使用匿名函数的方式实现map");

        env
                .addSource(new ClickSource())
                .map(new MapFunction<ClickEvent, String>() {
                    @Override
                    public String map(ClickEvent in) throws Exception {
                        return in.username;
                    }
                })
                .print("使用匿名内部类的方式实现map");

        env
                .addSource(new ClickSource())
                .map(new MyMap())
                .print("使用外部类的方式实现map");

        env
                .addSource(new ClickSource())
                .flatMap(new FlatMapFunction<ClickEvent, String>() {
                    @Override
                    public void flatMap(ClickEvent in, Collector<String> out) throws Exception {
                        // out收集数据向下游发送
                        out.collect(in.username);
                    }
                })
                .print("使用flatMap算子实现map");
        env.execute();
    }

    public static class MyMap implements MapFunction<ClickEvent, String> {
        @Override
        public String map(ClickEvent in) throws Exception {
            return in.username;
        }
    }
}
