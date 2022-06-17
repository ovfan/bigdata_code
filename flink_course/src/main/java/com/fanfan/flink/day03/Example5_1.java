package com.fanfan.flink.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * @ClassName: Example5_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月17日 21时43分
 * @Version: v1.0
 * TODO 实现并行数据源的富函数版本
 * 并行数据源决定了数据前往哪一个并行子任务中执行
 */
public class Example5_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new RichParallelSourceFunction<Integer>() {
            private boolean running = true;
            private Random random = new Random();

            // 1 -> 奇数
            // 0 -> 偶数
            @Override
            public void run(SourceContext<Integer> out) throws Exception {
                for (int i = 1; i < 10; i++) {
                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                        out.collect(i);
                        System.out.println("索引为" + getRuntimeContext().getIndexOfThisSubtask() + " 的数据- " + i + " -已发送");
                    }
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        })
                .setParallelism(2)
                .rebalance()
                .print()
                .setParallelism(4);

        env.execute();

    }
}
