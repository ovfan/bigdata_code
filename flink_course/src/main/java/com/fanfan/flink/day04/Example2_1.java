package com.fanfan.flink.day04;

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
 * @ClassName: Example2_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月21日 10时37分
 * @Version: v1.0
 * TODO 练习列表状态变量 -- 将数据流中来的每一条数据保存到列表状态变量中，实现奇偶数排序
 */
public class Example2_1 {
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

    public static class HistorySort extends KeyedProcessFunction<Integer, Integer, String> {
        // 声明列表状态状态变量
        private ListState<Integer> history;
        private ArrayList<Integer> result = new ArrayList<>();
        private StringBuilder resultTmp = new StringBuilder();

        @Override
        public void open(Configuration parameters) throws Exception {
            history = getRuntimeContext().getListState(new ListStateDescriptor<Integer>(
                    "history",
                    Types.INT
            ));
        }

        @Override
        public void processElement(Integer in, Context context, Collector<String> out) throws Exception {

            history.add(in);
            // 遍历列表状态变量中的值，保存到ArrayList中排序
            Iterable<Integer> integers = history.get();
            for (Integer integer : integers) {
                result.add(integer);
            }
            // 排序
            result.sort(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o1 - o2; // 升序排序
                }
            });

            // 格式化输出
            if (context.getCurrentKey() == 0) {
                resultTmp.append("偶数排序结果为: ");
            } else {
                resultTmp.append("奇数排序结果为: ");
            }
            for (Integer i : result) {
                resultTmp.append(i + "->");
            }
            out.collect(resultTmp.toString());

            //清空
            result.clear();
            // StringBuild 清空
            resultTmp.delete(0,resultTmp.length()-1);
        }
    }

    // 数据源
    public static class IntSource implements SourceFunction<Integer> {
        private boolean running = true;
        private Random random = new Random();

        // 生成数据，一秒生产一条
        @Override
        public void run(SourceContext<Integer> out) throws Exception {
            int randNUm;
            while (running) {
                randNUm = random.nextInt(1000);
                out.collect(randNUm);

                // 一秒发送一次
                Thread.sleep(1000L);
            }
        }

        // 取消生成数据
        @Override
        public void cancel() {

        }
    }
}
