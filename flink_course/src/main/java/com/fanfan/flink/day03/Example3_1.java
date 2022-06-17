package com.fanfan.flink.day03;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example3_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月17日 20时24分
 * @Version: v1.0
 * TODO 练习富函数的使用 -- 算子的每一个并行子任务都有自己的<span style="color:red">生命周期</span>。
 * 需求：求出数据源中的数据在flatmap哪一个并行子任务中读取到
 */
public class Example3_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .setParallelism(1)
                .flatMap(new RichFlatMapFunction<Integer, Integer>() {
                    // 富函数拥有生命周期
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // getRuntimeContext用来获取算子运行时的一些上下文信息
                        System.out.println("flatMap的并行子任务, " + getRuntimeContext().getIndexOfThisSubtask() + " ,生命周期开始");
                    }

                    @Override
                    public void flatMap(Integer in, Collector<Integer> out) throws Exception {
                        System.out.println("并行子任务的索引" + getRuntimeContext().getIndexOfThisSubtask() + "处理的数据是" + in);
                            // out.collect(in);
                    }

                    @Override
                    public void close() throws Exception {
                        System.out.println("flatMap的并行子任务，" + getRuntimeContext().getIndexOfThisSubtask() + " ,生命周期结束");
                    }
                })
                .setParallelism(2)
                .print()
                .setParallelism(4);

        env.execute();
    }
}
