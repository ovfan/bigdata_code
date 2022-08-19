package com.fanfan.tutoria.dat02;

import com.fanfan.tutoria.utils.IntPOJO;
import com.fanfan.tutoria.utils.IntSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example7_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月19日 16时17分
 * @Version: v1.0
 */
public class Example7_1 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new IntSource())
                .keyBy(r -> "int")
                .reduce(new ReduceFunction<IntPOJO>() {
                    @Override
                    public IntPOJO reduce(IntPOJO acc, IntPOJO in) throws Exception {
                        return new IntPOJO(
                                Math.min(in.min,acc.min),
                                Math.max(in.max,acc.max),
                                in.sum + acc.sum,
                                in.count + acc.count,
                                (in.sum + acc.sum) / (in.count + acc.count)
                        );
                    }
                })
                .print();

        env.execute();
    }
}
