package com.fanfan.tutoria.day04;


import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;

/**
 * @ClassName: Example1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月21日 09时29分
 * @Version: v1.0
 */
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new newIntSource())
                .keyBy(new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer in) throws Exception {
                        return in % 2;
                    }
                })
                .process(new HistoryDataSort())
                .print();

        env.execute();
    }

    public static class HistoryDataSort extends KeyedProcessFunction<Integer, Integer, String> {
        // 声明一个列表状态变量 保存奇 偶 历史数据
        private ListState<Integer> history;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化列表状态变量
            history = getRuntimeContext().getListState(new ListStateDescriptor<Integer>(
                    "history",
                    Types.INT
            ));
        }

        @Override
        public void processElement(Integer in, Context ctx, Collector<String> out) throws Exception {
            // 保存排序结果
            StringBuilder result = new StringBuilder();

            // 将in添加到historyData中in的key对应的ArrayList中
            history.add(in);

            if (ctx.getCurrentKey() == 0) {
                result.append("偶数排序结果: ");
            } else if (ctx.getCurrentKey() == 1) {
                result.append("奇数排序结果: ");
            }

            ArrayList<Integer> integers = new ArrayList<>();
            for (Integer i : history.get()) {
                integers.add(i);
            }
            // 对集合进行升序排序
            integers.sort(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o1 - o2;
                }
            });

            for (Integer n : integers) {
                result.append(n + " => ");
            }
            out.collect(result.toString());
        }

    }

    public static class newIntSource implements SourceFunction<Integer> {
        private Boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                ctx.collect(random.nextInt(1000));
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
