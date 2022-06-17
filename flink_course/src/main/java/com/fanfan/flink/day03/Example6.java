package com.fanfan.flink.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @ClassName: Example6
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月17日 12时21分
 * @Version: v1.0
 * TODO 物理分区算子rescale 与 rebalance区别
 * rescale将数据轮询发送到下游的部分并行子任务中。用在下游算子的并行度是上游算子的并行度的整数倍的情况。round-robin
 */
public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new RichParallelSourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        int i = 1;
                        while (i < 9) {
                            i++;
                            // 并行子任务索引为0的发送偶数
                            // 并行子任务索引为1的发送奇数
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
                })
                .setParallelism(2)
                .rescale()
                .print()
                .setParallelism(4);

        env.execute();
    }
}
