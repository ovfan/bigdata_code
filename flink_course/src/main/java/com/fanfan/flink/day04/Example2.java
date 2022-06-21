package com.fanfan.flink.day04;

import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;

/**
 * @ClassName: Example2
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月18日 11时05分
 * @Version: v1.0
 * TODO 练习列表状态变量ListStat
 * 需求：对数据源采集来的数字数据 进行排序输出
 */
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new IntSource())
                .keyBy(r -> r % 2)
                .process(new HistorySort())
                .print();
        env.execute();
    }
    // 注：分组字段为 0 与 1 是Integer类型
    public static class HistorySort extends KeyedProcessFunction<Integer,Integer,String>{
        private ListState<Integer> history;
        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化列表状态变量
            history = getRuntimeContext().getListState(
                    new ListStateDescriptor<Integer>(
                            "history",
                            Types.INT
                    )
            );
        }
        @Override
        public void processElement(Integer in, Context context, Collector<String> out) throws Exception {
            Integer key = context.getCurrentKey();
            ArrayList<Integer> integers = new ArrayList<>();

            history.add(in);
            for (Integer num : history.get()) {
                integers.add(num);
            }
            integers.sort(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    // 升序 o1 -o2
                    return o1 -o2;
                }
            });

            // 格式化输出
            StringBuilder result = new StringBuilder();
            if(key == 0){
                result.append("偶数的排序结果为: " );
            }else{
                result.append(("奇数的排序结果为: "));
            }

            for (Integer i : integers) {
                result.append(i + " -> ");
            }

            out.collect(result.toString());
        }
    }

    public static class IntSource implements SourceFunction<Integer> {
        private boolean running = true;
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
