package com.fanfan.flink.day03;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example3
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月17日 10时50分
 * @Version: v1.0
 * TODO 练习富函数的使用 -- 算子的每一个并行子任务都有自己的<span style="color:red">生命周期</span>。
 */
public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //需求：求出数据源--在哪个flatMap的并行子任务执行
        env
                .fromElements(1, 2, 3, 4)
                .setParallelism(1)
                .flatMap(
                        new RichFlatMapFunction<Integer, Integer>() {
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                System.out.println("flatMap的并行子任务：" + getRuntimeContext().getIndexOfThisSubtask()
                                        + "生命周期开始");

                            }

                            @Override
                            public void flatMap(Integer in, Collector<Integer> collector) throws Exception {
                                System.out.println("并行子任务索引:" + getRuntimeContext().getIndexOfThisSubtask() + " ，处理的数据是" + in);
                            }

                            @Override
                            public void close() throws Exception {
                                System.out.println("flatMap的并行子任务:" + getRuntimeContext().getIndexOfThisSubtask()
                                        + "生命周期结束");
                            }
                        })
                .setParallelism(2)
                .print()
                .setParallelism(2);


        env.execute();
    }
}
