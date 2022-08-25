package com.fanfan.tutoria.revise;

import com.fanfan.flink.day04.Example2_1;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/**
 * @ClassName: MyListStateExample
 * @Description: TODO 适用列表状态变量 完成 历史数据排序的功能
 * @Author: fanfan
 * @DateTime: 2022年08月25日 12时45分
 * @Version: v1.0
 */
public class MyListStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new Example2_1.IntSource())
                .setParallelism(1)
                .keyBy(r -> r % 2)
                .process(new MyHistorySort())
                .setParallelism(2)
                .print()
                .setParallelism(2);

        env.execute();
    }

    public static class MyHistorySort extends KeyedProcessFunction<Integer, Integer, String> {
        // 定义列表状态变量 保存历史数据
        private ListState<Integer> history;

        @Override
        public void open(Configuration parameters) throws Exception {
            history = getRuntimeContext().getListState(new ListStateDescriptor<Integer>(
                    "history",
                    Types.INT
            ));
        }

        @Override
        public void processElement(Integer in, Context ctx, Collector<String> out) throws Exception {
            StringBuilder result = new StringBuilder();
            if (ctx.getCurrentKey() == 0) {
                result.append("偶数历史数据排序结果: ");
            } else {
                result.append("奇数历史数据排序结果: ");
            }
            history.add(in);

            ArrayList<Integer> resultSort = new ArrayList<>();
            for (Integer i : history.get()) {
                resultSort.add(i);
            }
            resultSort.sort(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o1 -o2;
                }
            });
            Iterator<Integer> iterator = resultSort.iterator();
            while (iterator.hasNext()) {
                result.append(iterator.next() + " => ");
            }
            out.collect(result.toString());
        }
    }
}
