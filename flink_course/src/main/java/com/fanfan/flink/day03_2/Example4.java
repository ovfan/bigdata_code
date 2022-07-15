package com.fanfan.flink.day03_2;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @ClassName: Example4
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月15日 19时22分
 * @Version: v1.0
 */
public class Example4 {
    public static void main(String[] args) throws Exception {
        // TODO 练习并行数据源
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new RichParallelSourceFunction<String>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {

                    }

                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        //TODO 1,3 5 7 发往source的第一个并行子任务中
                        //     2,4 6 8 发往source的第二个并行子任务中
                        for (int i = 1; i < 9; i++) {
                            if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                                ctx.collect("数据:" + i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {

                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                    }
                })
                .setParallelism(2)
                .print()
                .setParallelism(2);

        env.execute();
    }
}
