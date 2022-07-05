package com.fanfan.flink.day03_1;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example4
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月05日 16时35分
 * @Version: v1.0
 */
public class Example4 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // TODO 测试keyby的数据 在reduce的哪个并行子任务中执行
        env
                .fromElements(1,2,3,4,5,6,7,8,9)
                .keyBy(new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer in) throws Exception {
                        return in % 3;
                    }
                })
                .reduce(new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer acc, Integer in) throws Exception {
                        return acc + in;
                    }
                })
                .setParallelism(4)
                .print()
                .setParallelism(4);

        env.execute();
    }
}
