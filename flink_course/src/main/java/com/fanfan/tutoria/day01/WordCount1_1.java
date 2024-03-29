package com.fanfan.tutoria.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: WordCount1_1
 * @Description: TODO 实现wordcount - 复习
 * @Author: fanfan
 * @DateTime: 2022年08月17日 12时02分
 * @Version: v2.0 -- 升级读取配置文件
 */
public class WordCount1_1 {
    public static void main(String[] args) throws Exception {
        // 1. 准备流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从参数中读取主机和端口号
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        Integer port = parameterTool.getInt("port");

        // 从配置文件中读取
        ParameterTool parameterTool1 = ParameterTool.fromPropertiesFile("flink_course/src/main/resources/info.properties");
        String host1 = parameterTool1.get("host");
        String port1 = parameterTool1.get("port");
        System.out.println("port1 = " + port1);
        // 2. 定义数据的有向无环图DAG
        env
                .socketTextStream(host, port)
                .setParallelism(1)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] words = in.split("\\s+");
                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                .setParallelism(1)
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

                    @Override
                    public String getKey(Tuple2<String, Integer> in) throws Exception {
                        return in.f0;
                    }
                })
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> acc, Tuple2<String, Integer> in) throws Exception {
                        return Tuple2.of(in.f0, in.f1 + acc.f1);
                    }
                })
                .print()
                .setParallelism(1);

        // 3. 提交并执行程序(DAG)
        env.execute();
    }
}
