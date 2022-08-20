package com.fanfan.tutoria.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example7
 * @Description: TODO 使用processFunction 实现flatMap
 * @Author: fanfan
 * @DateTime: 2022年08月20日 14时11分
 * @Version: v1.0
 */
public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .fromElements("white", "black", "blue")
                .process(new ProcessFunction<String, String>() {

                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        if (in.equals("white")) {
                            out.collect(in);
                        } else if (in.equals("black")) {
                            out.collect(in);
                            out.collect(in);
                        }
                    }
                })
                .print();
        env.execute();
    }
}
