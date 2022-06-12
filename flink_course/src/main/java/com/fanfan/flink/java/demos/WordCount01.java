package com.fanfan.flink.java.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: WordCount01
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月12日 20时58分
 * @Version: v1.0
 */
public class WordCount01 {
    public static void main(String[] args) throws Exception {
        // 使用flink框架完成wordcount案例开发，需求：以nc产生的数据为数据源
        // 1. 准备flink环境对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 监听nc 9999端口数据
        DataStreamSource<String> words = env.socketTextStream("192.168.202.102", 9999);

        // 3. 处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = words.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split("\\s+");
                for (String word : split) {
                    // 返回每对单词(word,1)
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });
        // 通过sink算子，将数据写出
        wordToOne.print();

        // 触发程序的提交运行
        env.execute();

    }
}
