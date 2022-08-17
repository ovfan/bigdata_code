package com.fanfan.tutoria.day01;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: WordCount1_2
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月17日 13时51分
 * @Version: v1.0
 */
public class WordCount1_2 {
    public static void main(String[] args) throws Exception {
        // 1. 准备流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2.1 读取文件
        DataStreamSource<String> lineDataStreamSource = env.readTextFile("flink_course/src/main/resources/word.txt");

        // 2.2 转换计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lineDataStreamSource.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = line.split("\\s+");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        // 2.3 单词分组
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneGroup = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> in) throws Exception {
                return in.f0;
            }
        });

        // 2.4 聚合计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount = wordAndOneGroup.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> acc, Tuple2<String, Integer> in) throws Exception {
                return Tuple2.of(in.f0, acc.f1 + in.f1);
            }
        });

        // 2.5 打印输出
        wordCount.print();

        // 3. 提交并执行程序
        env.execute();
    }
}
