package com.fanfan.flink.day03;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @ClassName: Example7_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月17日 22时04分
 * @Version: v1.0
 * "向下游发送的数据为:" + word + " ,发生时间为:" + new Timestamp(ts)
 * TODO 使用Flink底层API实现FlatMap的复制，过滤，和map功能
 */
public class Example7_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .socketTextStream("hadoop102", 9999)
                .process(new ProcessFunction<String, Tuple2<String, String>>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {

                    }

                    @Override
                    public void processElement(String in, Context context, Collector<Tuple2<String, String>> out) throws Exception {
                        String[] words = in.split("\\s+");
                        long currTs = context.timerService().currentProcessingTime(); // 数据发送的 事件时间
                        for (String word : words) {
                            if (word.equals("white")) {
                                out.collect(Tuple2.of(word, null));
                                System.out.println("向下游发送的数据为：" + word + "发送的时间为" + new Timestamp(currTs));
                            } else if (word.equals("blue")) {
                                out.collect(Tuple2.of(word, word));
                                System.out.println("向下游发送的数据为：" + word + "发送的时间为" + new Timestamp(currTs));
                            }
                        }

                    }

                    @Override
                    public void close() throws Exception {

                    }
                }).setParallelism(1)
                .print().setParallelism(1);

        env.execute();

    }
}
