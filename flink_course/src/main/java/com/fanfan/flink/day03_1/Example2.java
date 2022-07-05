package com.fanfan.flink.day03_1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example2
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月05日 15时12分
 * @Version: v1.0
 * TODO 练习map的使用--过滤出clicksource中的用户名
 */
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .map(r -> r.user)
                .print("使用匿名函数的方式");
        env
                .addSource(new ClickSource())
                .map(new MyMap())
                .print("使用外部类的方式");

        env
                .addSource(new ClickSource())
                .map(new MapFunction<ClickEvent, String>() {
                    @Override
                    public String map(ClickEvent clickEvent) throws Exception {
                        return clickEvent.user;
                    }
                })
                .print("使用匿名内部类的方式");
        env
                .addSource(new ClickSource())
                .flatMap(new FlatMapFunction<ClickEvent, String>() {
                    @Override
                    public void flatMap(ClickEvent in, Collector<String> out) throws Exception {
                        out.collect(in.user);
                    }
                })
                .print("使用flatmap的方式");
        env.execute();
    }
    public static class MyMap implements MapFunction<ClickEvent,String>{
        @Override
        public String map(ClickEvent clickEvent) throws Exception {
            return clickEvent.user;
        }
    }
}
