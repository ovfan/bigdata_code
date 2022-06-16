package com.fanfan.flink.day02;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example9
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月16日 22时41分
 * @Version: v1.0
 */
public class Example9 {
    public static void main(String[] args) throws Exception {
        // 从数组中读取一组数据，查看其key对应的累加器在reduce哪个并行子任务中执行
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .keyBy(r -> r % 3)
                .reduce(new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer in, Integer out) throws Exception {
                        return in + out;
                    }
                })
                .setParallelism(4)
                .print()
                .setParallelism(4);
                //  3> 1
                //  4> 2
                //  3> 3
                //  3> 5
                //  4> 7
                //  3> 9
                //  4> 15
                //  3> 12
                //TODO key为1与key为0的聚合到reduce并行子任务索引[2]处执行
                // key为2的聚合到reduce并行子任务索引[3]处执行

        env.execute();
    }
}
