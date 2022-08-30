package com.fanfan.tutoria.day06;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName: Example5_2
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月30日 11时47分
 * @Version: v1.0
 */
public class Example5_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 测试 迟到数据发送到 侧输出流中
        SingleOutputStreamOperator<String> result = env
                .addSource(new SourceFunction<String>() {

                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        ctx.collectWithTimestamp("a", 1000L);
                        ctx.collectWithTimestamp("a", 3000L);
                        ctx.emitWatermark(new Watermark(19999L));
                        ctx.collectWithTimestamp("a", 5000L);
                    }

                    @Override
                    public void cancel() {

                    }
                }).process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        if (ctx.timestamp() < ctx.timerService().currentWatermark()) {
                            // 如果输入事件所携带的事件时间时间戳 小于 当前水位线的时间戳，说明该数据是迟到数据
                            ctx.output(new OutputTag<String>("late-event") {
                                       },
                                    // 向下游发送的数据
                                    "数据:" + in + ctx.timestamp() + "迟到了"
                            );
                        } else {
                            out.collect("数据:" + in + ctx.timestamp() + "没迟到");
                        }
                    }
                });

        result.print("主流:");
        result.getSideOutput(new OutputTag<String>("late-event") {
        }).print();
        env.execute();
    }
}
