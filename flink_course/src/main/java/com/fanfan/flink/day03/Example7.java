package com.fanfan.flink.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @ClassName: Example7
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月17日 14时59分
 * @Version: v1.0
 * DataStream 底层API processFunction<String,String> 输入数据、输出数据</>
 */
public class Example7 {
    public static void main(String[] args) throws Exception {
        // 使用processFunction实现FlatMap的功能
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .socketTextStream("hadoop102", 9999)
                .process(new ProcessFunction<String, String>() {
                             // 对每一条输入的数据进行处理
                             @Override
                             public void processElement(String in, Context context, Collector<String> out) throws Exception {
                                 String[] words = in.split("\\s+");

                                 for (String word : words) {
                                     long ts = context.timerService().currentProcessingTime(); // 事件时间
                                     if (word.equals("white")) {
                                         out.collect("向下游发送的数据为:" + word + " ,发生时间为:" + new Timestamp(ts));
                                     } else if (word.equals("black")) {
                                         out.collect("向下游发送的数据为:" + word + " ,发生时间为:" + new Timestamp(ts));
                                         out.collect("向下游发送的数据为:" + word + " ,发生时间为:" + new Timestamp(ts));
                                     }
                                 }
                             }
                         }
                )
                .print();

        env.execute();
    }
}
