package com.fanfan.tutoria.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @ClassName: Example4
 * @Description: TODO 定义并行数据源 -- 富函数版本
 * @Author: fanfan
 * @DateTime: 2022年08月20日 10时16分
 * @Version: v1.0
 */
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                // 每个并行子任务 都会执行一遍
                .addSource(new RichParallelSourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        for (int i = 1; i < 9; i++) {
                            ctx.collect("并行子任务索引:" + getRuntimeContext().getIndexOfThisSubtask() + "发送数据:" + i);
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .setParallelism(2)
                .print()
                .setParallelism(2);

        env.execute();
    }
}
