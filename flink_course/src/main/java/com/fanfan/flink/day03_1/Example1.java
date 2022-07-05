package com.fanfan.flink.day03_1;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月05日 15时05分
 * @Version: v1.0
 */
public class Example1 {
    public static void main(String[] args) throws Exception{
        // TODO 打印数据源中过来的数据

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new ClickSource())
                .setParallelism(1)
                .print()
                .setParallelism(1);

        // 提交任务并执行
        env.execute();
    }
}
