package com.fanfan.tutoria.day01;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: WordCount1_3
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月17日 14时09分
 * @Version: v1.0
 */
public class WordCount1_3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从参数中提取主机名 和 端口号
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("host");
        Integer port = parameterTool.getInt("port");
        // 读取文本流
        env.socketTextStream(hostname, port).setParallelism(1)
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    String[] words = line.split("\\s+");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .setParallelism(1)
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> tuple) throws Exception {
                        return tuple.f0;
                    }
                })
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> acc, Tuple2<String, Integer> in) throws Exception {
                        return Tuple2.of(in.f0, in.f1 + acc.f1);
                    }
                })
                .setParallelism(1)
                .print().setParallelism(1);

        env.execute();
    }
}
