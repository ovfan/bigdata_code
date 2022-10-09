package com.fanfan.flink.day01;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @ClassName: Example5
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年10月09日 16时52分
 * @Version: v1.0
 */
public class Example5 {
    //TODO 自定义数据源
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new MySource())
                // SourceFunction<T> 默认并行度是1，如果需要多并行度，
                //          使用ParallelSourceFunction<T>

                .print();

        env.execute();
    }
    public static class MySource implements SourceFunction<Integer>{
        private boolean running = true;
        private Random random = new Random();
        // 数据产生的数据 通过ctx收集发送
        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while(running){
                Integer randNum = random.nextInt(1000);
                ctx.collect(randNum);
                Thread.sleep(1000L);
            }
        }
        @Override
        public void cancel() {
            running = false;
        }
    }
}
