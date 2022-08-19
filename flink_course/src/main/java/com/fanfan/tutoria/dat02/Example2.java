package com.fanfan.tutoria.dat02;

import com.fanfan.tutoria.utils.ClickEvent;
import com.fanfan.tutoria.utils.ClickSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example2
 * @Description: TODO map算子使用
 *                  打印点击流中 用户的姓名
 * @Author: fanfan
 * @DateTime: 2022年08月19日 14时16分
 * @Version: v1.0
 */
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置全局并行度
        env.setParallelism(1);
       env
               .addSource(new ClickSource())
               .map(new MapFunction<ClickEvent, String>() {
                   @Override
                   public String map(ClickEvent in) throws Exception {
                       return in.username;
                   }
               })
               .print("使用匿名类实现map");

        env
                .addSource(new ClickSource())
                .map(new MyMap())
                .print("使用外部类实现map");
        env
                .addSource(new ClickSource())
                .map(r -> r.username)
                .print("使用匿名类实现map");

        env
                .addSource(new ClickSource())
                .flatMap(new FlatMapFunction<ClickEvent, String>() {
                    @Override
                    public void flatMap(ClickEvent in, Collector<String> out) throws Exception {
                        out.collect(in.username);
                    }
                })
                .print("使用flatMap实现map");
        env.execute();
    }
    public static class MyMap implements MapFunction<ClickEvent,String>{
        @Override
        public String map(ClickEvent in) throws Exception {
            return in.username;
        }
    }


}
