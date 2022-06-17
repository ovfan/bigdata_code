package com.fanfan.flink.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @ClassName: Example11
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月16日 23时14分
 * @Version: v1.0
 * TODO 需求：从数据源获取不间断的 数字，计算其最大值，最小值，累加值，出现次数，平均数
 */
public class Example11 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //自定义数据源需要实现两个方法，run与cancel
        env
                .addSource(new SourceFunction<Integer>() {
                    private Random random = new Random();
                    private boolean running = true;

                    @Override
                    public void run(SourceContext<Integer> src) throws Exception {
                        while (running) {
                            src.collect(random.nextInt(1000));
                            Thread.sleep(1000);
                        }

                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .map(new MapFunction<Integer, Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Integer in) throws Exception {
                        return Tuple5.of(
                                in,
                                in,
                                in,
                                1,
                                in
                        );
                    }
                })
                .keyBy(in -> "int") //分到一个组中
                .reduce(new Statistic())
                .print();


        env.execute();
    }

    //TODO 统计类，计算最大值，最小值，累加值，出现次数，平均数
    public static class Statistic implements ReduceFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>> {
        // reduce方法的第一个参数是累加器，第二个参数是输入参数
        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Integer> reduce(Tuple5<Integer, Integer, Integer, Integer, Integer> acc, Tuple5<Integer, Integer, Integer, Integer, Integer> in) throws Exception {
            return Tuple5.of(
                    Math.max(in.f0, acc.f0),    // 最大值
                    Math.min(in.f1, acc.f1),     // 最小值
                    in.f2 + acc.f2,             // 累加值
                    in.f3 + acc.f3,             // 总条数--出现次数
                    (in.f2 + acc.f2) / (in.f3 + acc.f3)     // 平均数
            );
        }
    }
}
