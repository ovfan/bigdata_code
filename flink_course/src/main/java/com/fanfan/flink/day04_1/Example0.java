package com.fanfan.flink.day04_1;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @ClassName: Example0
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月17日 08时29分
 * @Version: v1.0
 * TODO 值状态变量练习--自定义Int数据源，进行整数流的统计
 */
public class Example0 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new IntSource())
                .setParallelism(2)
                .keyBy(r -> r %2)
                .print();


        env.execute();
    }
    public static class IntSource implements ParallelSourceFunction<Integer>{
        private Random random = new Random();
        private boolean running = true;
        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running){
                int randNum = random.nextInt(1000);
                ctx.collect(randNum);
            }
        }

        @Override
        public void cancel() {

        }
    }
}
