package com.fanfan.flink.day03_2;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example6
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月16日 08时33分
 * @Version: v1.0
 */
public class Example6 {
    // TODO 使用底层api 实现flatMap功能
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .fromElements("white","black","blue")
                .process(new ProcessFunction<String, Tuple2<String,Integer>>(){
                    @Override
                    public void open(Configuration parameters) throws Exception {

                    }

                    @Override
                    public void processElement(String in, Context ctx, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        if(in.equals("white")){
                            collector.collect(Tuple2.of("white",1));
                        }else if(in.equals("black")){
                            collector.collect(Tuple2.of("black",2));
                        }
                    }

                    @Override
                    public void close() throws Exception {

                    }
                })
                .setParallelism(1)
                .print();

        env.execute();
    }
}
