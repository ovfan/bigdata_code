package com.fanfan.flink.day03_2;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example5
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月15日 20时31分
 * @Version: v1.0
 */
public class Example5 {
    public static void main(String[] args) throws Exception{
        //TODO 练习底层API ProcessFunction<in,out> KeyedProcessFunction<key,in,out>
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env
                .fromElements("white","black","blue")
                .process(new ProcessFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        if(in.equals("white")){
                            out.collect(Tuple2.of(in,1));
                        }else if(in.equals("black")){
                            out.collect(Tuple2.of(in,2));
                        }
                    }
                })
                .print()
                .setParallelism(1);

        env.execute();
    }
}
