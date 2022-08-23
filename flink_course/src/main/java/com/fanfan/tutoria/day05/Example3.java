package com.fanfan.tutoria.day05;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName: Example3
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月23日 13时57分
 * @Version: v1.0
 */
public class Example3 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .socketTextStream("hadoop102",9999)
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String in) throws Exception {
                        String[] array = in.split(" ");
                        return Tuple2.of(array[0],Long.parseLong(array[1]) * 1000L);
                    }
                });
                // 在map输出的数据流中插入水位线事件
                // 默认是200ms 插入一次




        env.execute();
    }
}
