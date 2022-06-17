package com.fanfan.flink.day02;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example11_2
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月17日 10时13分
 * @Version: v1.0
 * 练习map与reduce
 */
public class Example11_2 {
//    public static void main(String[] args) throws Exception{
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        env
//                .fromElements(1,2,3,4,5,6,7,8)
//                .map(r -> Tuple5.of(r,r,r,1,r))
//                .returns(Types.TUPLE(
//                        Types.INT,
//                        Types.INT,
//                        Types.INT,
//                        Types.INT,
//                        Types.INT
//                        ))
//                .keyBy(k -> "int")
//                .reduce(new ReduceFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
//                    @Override
//                    public Tuple5<Integer, Integer, Integer, Integer, Integer> reduce(Tuple5<Integer, Integer, Integer, Integer, Integer> acc, Tuple5<Integer, Integer, Integer, Integer, Integer> in) throws Exception {
//                        return null;
//                    }
//                })
//
//        env.execute();
//    }
}
