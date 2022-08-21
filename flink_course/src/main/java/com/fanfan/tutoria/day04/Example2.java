package com.fanfan.tutoria.day04;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * @ClassName: Example2
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月21日 10时23分
 * @Version: v1.0
 */
public class Example2 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new Example1.newIntSource())
                .keyBy(r -> r%2)
                .process(new KeyedProcessFunction<Integer, Integer, String>() {
                    private ListState<Integer>  historyData;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        historyData = getRuntimeContext().getListState(new ListStateDescriptor<Integer>(
                                "history",
                                Types.INT
                        ));
                    }

                    @Override
                    public void processElement(Integer in, Context ctx, Collector<String> out) throws Exception {
                        StringBuilder result = new StringBuilder();
                        historyData.add(in);
                        if (ctx.getCurrentKey() == 0){
                            result.append("偶数排序结果为: ");
                        }else{
                            result.append("奇数排序结果为: ");
                        }
                        ArrayList<Integer> integers = new ArrayList<>();
                        for (Integer i : historyData.get()) {
                            integers.add(i);
                        }
                        integers.sort(new Comparator<Integer>() {
                            @Override
                            public int compare(Integer o1, Integer o2) {
                                return o1 - o2;
                            }
                        });

                        for (Integer n : integers) {
                            result.append(n + " => ");
                        }
                        out.collect(result.toString());
                    }
                }).print();


        env.execute();
    }
}
