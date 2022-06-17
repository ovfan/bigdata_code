package com.fanfan.flink.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @ClassName: Example5
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月17日 12时14分
 * @Version: v1.0
 * TODO 实现并行数据源的富函数版本
 */
public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new RichParallelSourceFunction<Integer>() {

                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        //TODO 1,3 5 7 发往source的第一个并行子任务中
                        //     2,4 6 8 发往source的第二个并行子任务中
                        for (int i = 1; i < 9; i++) {
                            if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                                ctx.collect(i);
                                System.out.println("source并行子任务的索引为:" + getRuntimeContext().getIndexOfThisSubtask()
                                        + "发送的数据为:" + i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                }).setParallelism(2)
                .rebalance()
                .print().setParallelism(4);

        env.execute();
    }
}
