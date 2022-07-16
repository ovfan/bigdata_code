package com.fanfan.flink.day03_2;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example8
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月16日 09时13分
 * @Version: v1.0
 */
public class Example8 {
    // TODO 练习值状态变量 完成IntStatistic统计计算
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new SourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> src) throws Exception {
                        for (int i = 1; i < 11; i++) {
                            src.collect(i);
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r -> "int")
                .process(new KeyedProcessFunction<String, Integer, Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
                    private ValueState<Tuple5<Integer, Integer, Integer, Integer, Integer>> acc; // 定义值状态变量

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 针对每个值状态变量取一个唯一的描述符(name)
                        acc = getRuntimeContext().getState(
                          new ValueStateDescriptor<Tuple5<Integer, Integer, Integer, Integer, Integer>>(
                                  "acc",
                                  Types.TUPLE(Types.INT,Types.INT,Types.INT,Types.INT,Types.INT)
                          )
                        );
                    }
                    @Override
                    public void close() throws Exception {

                    }

                    @Override
                    public void processElement(Integer in, Context context, Collector<Tuple5<Integer, Integer, Integer, Integer, Integer>> out) throws Exception {
                        Tuple5<Integer, Integer, Integer, Integer, Integer> oldAcc = acc.value();
                        // 初始化值状态变量
                        if(acc.value() == null){
                            // 为空则初始化值状态变量
                            acc.update(Tuple5.of(in,in,in,1,in));

                        }else{
                            acc.update(Tuple5.of(
                                    Math.min(in, oldAcc.f0),
                                    Math.max(in,oldAcc.f1),
                                    in + oldAcc.f2,
                                    1 + oldAcc.f3,
                                    (in + oldAcc.f2) / (1 + oldAcc.f3)
                            ));
                        }
                        // 每来一条数据想下游发送数据
                        out.collect(acc.value());
                    }
                })
                .print();


        env.execute();
    }
}
