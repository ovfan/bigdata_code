package com.fanfan.tutoria.day07;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example4
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月26日 16时09分
 * @Version: v1.0
 */
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Tuple2<String, String>> stream1 = env.fromElements(
                Tuple2.of("a", "left1"),
                Tuple2.of("b", "left1"),
                Tuple2.of("a", "left2"),
                Tuple2.of("b", "left2")
                );

        DataStreamSource<Tuple2<String, String>> stream2 = env.fromElements(
                Tuple2.of("a", "right1"),
                Tuple2.of("b", "right1"),
                Tuple2.of("a", "right2"),
                Tuple2.of("b", "right2")
        );

        stream1
                .keyBy(r -> r.f0)
                .connect(stream2.keyBy(r -> r.f0))
                .process(new InnerJoin())
                .print();

        env.execute();
    }
    public static class InnerJoin extends CoProcessFunction<Tuple2<String,String>,Tuple2<String,String>,String>{
        private ListState<Tuple2<String,String>> history1;
        private ListState<Tuple2<String,String>> history2;

        @Override
        public void open(Configuration parameters) throws Exception {
            history1 = getRuntimeContext().getListState(
                    new ListStateDescriptor<Tuple2<String, String>>(
                            "history1",
                            Types.TUPLE(Types.STRING, Types.STRING)
                    )
            );
            history2 = getRuntimeContext().getListState(
                    new ListStateDescriptor<Tuple2<String, String>>(
                            "history2",
                            Types.TUPLE(Types.STRING, Types.STRING)
                    )
            );
        }

        @Override
        public void processElement1(Tuple2<String, String> in1, Context ctx, Collector<String> out) throws Exception {
            history1.add(in1);
        }

        @Override
        public void processElement2(Tuple2<String, String> in2, Context ctx, Collector<String> out) throws Exception {

        }
    }
}
