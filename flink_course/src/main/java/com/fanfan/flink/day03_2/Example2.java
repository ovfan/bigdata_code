package com.fanfan.flink.day03_2;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example2
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月15日 16时58分
 * @Version: v1.0
 */
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // TODO 练习富函数的使用
        env
                .fromElements(1,2,3,4)
                .flatMap(new RichFlatMapFunction<Integer, String>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //TODO 获取发往下游哪一个并行子任务的索引 (前提是：flatMap已经设置不为1的并行度)
                        System.out.println("发往print并行子任务索引为: "+getRuntimeContext().getIndexOfThisSubtask() + "中运行" + ", 生命周期开始");
                    }

                    @Override
                    public void flatMap(Integer in, Collector<String> out) throws Exception {
                        out.collect("发送的数据为:" + in + "在print并行子任务" + getRuntimeContext().getIndexOfThisSubtask() + "中运行");
                    }

                    @Override
                    public void close() throws Exception {
                        System.out.println("索引:" + getRuntimeContext().getIndexOfThisSubtask() + "生命周期结束");
                    }
                })
                .setParallelism(2) //flatMap要设置并行度
                .print()
                .setParallelism(4);
        env.execute();
    }
}
