package com.fanfan.flink.day03_1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @ClassName: Example5
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月05日 16时39分
 * @Version: v1.0
 */
public class Example5 {
    public static void main(String[] args) throws Exception{
        //TODO 求最大值......
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new SourceFunction<Integer>() {
                    private boolean running = true;
                    private Random random = new Random();
                    // 随机生成数字
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        while(running){
                            ctx.collect(random.nextInt(1000));
                            Thread.sleep(1000L);
                        }
                    }
                    @Override
                    public void cancel() {
                      running = false;
                    }
                })
                .map(new MapFunction<Integer, Tuple5<Integer,Integer,Integer,Integer,Integer>>() {
                    @Override
                    public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Integer in) throws Exception {
                        return Tuple5.of(in,in,in,1,in);
                    }
                })
                .keyBy(r -> "int")
                .reduce(new Statistic())
                .setParallelism(1)
                .print()
                .setParallelism(1);
        env.execute();
    }
    public static class Statistic implements ReduceFunction<Tuple5<Integer,Integer,Integer,Integer,Integer>>{
        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Integer> reduce(Tuple5<Integer, Integer, Integer, Integer, Integer> acc, Tuple5<Integer, Integer, Integer, Integer, Integer> in) throws Exception {

            return Tuple5.of(
                    Math.min(acc.f0,in.f0),
                    Math.max(acc.f1,in.f1),
                    acc.f2 + in.f2,
                    acc.f3 + in.f3,
                    (acc.f2 + in.f2) / (acc.f3 + in.f3)
            );
        }
    }
}
