package com.fanfan.tutoria.day03;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example7_1
 * @Description: TODO 使用KeyedProcessFunction 实现flatMap功能
 * @Author: fanfan
 * @DateTime: 2022年08月20日 14时38分
 * @Version: v1.0
 */
public class Example7_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .fromElements("white", "black", "blue")
                // KeySelector<IN, KEY>
                .keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String in) throws Exception {
                        return "string";
                    }
                })
                .process(new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        if (in.equals("white")) {
                            out.collect(in);
                        } else if (in.equals("black")) {
                            out.collect("哈哈哈🎉");
                            out.collect("哈哈哈🎉");
                        }
                    }
                })
                .print();


        env.execute();
    }
}
