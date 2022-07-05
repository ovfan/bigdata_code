package com.fanfan.flink.day03_1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example3_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月05日 15时24分
 * @Version: v1.0
 */
public class Example3_2 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .fromElements("white","black","blue")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String in, Collector<String> out) throws Exception {
                        if(in.equals("white")){
                            out.collect(in);
                        }else if(in.equals("black")){
                            out.collect("black");
                            out.collect("black");
                        }
                    }
                })
                .print();

        env
                .addSource(new ClickSource())
                .filter(r -> r.user.equals("fan"))
                .print();

        env.execute();
    }
}
