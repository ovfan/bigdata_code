package com.fanfan.tutoria.day06;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName: Example5_1
 * @Description: TODO flink处理迟到数据的策略 测试
 * @Author: fanfan
 * @DateTime: 2022年08月30日 11时13分
 * @Version: v1.0
 */
public class Example5_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<String> result = env
                .addSource(new SourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        ctx.collectWithTimestamp("a", 1000L);
                        ctx.collectWithTimestamp("a", 3000L);
                        ctx.emitWatermark(new Watermark(19999L));
                        ctx.collectWithTimestamp("a", 9000L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        if (ctx.timestamp() < ctx.timerService().currentWatermark()) {
                            // 说明是迟到数据,需要发送到侧输出流
                            // 侧输出流的名字
                            ctx.output(new OutputTag<String>("late-event") {
                                       }
                                    ,
                                    // 向侧输出流中发送的数据
                                    "数据" + in + "," + ctx.timestamp() + "迟到了"
                            );
                        }else {
                            out.collect("数据" + in + "," + ctx.timestamp() + "没到了");
                        }
                    }
                });

                result.print("主流:");
                result.getSideOutput(new OutputTag<String>("late-event"){}).print("侧流:");

        env.execute();
    }
}
