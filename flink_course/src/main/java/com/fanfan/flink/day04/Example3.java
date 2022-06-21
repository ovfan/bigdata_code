package com.fanfan.flink.day04;

import com.fanfan.flink.utils.ClickEvent;
import com.fanfan.flink.utils.ClickSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example3
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月18日 11时26分
 * @Version: v1.0
 * TODO 1. 通过以往知识实现 用户访问的页面统计 user-url-count
 * 结构为：Tuple2(Tuple2(username,url),count)
 */
public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //添加点击流数据源
        env
                .addSource(new ClickSource())
                // TODO 根据用户名 来分组
                // KeySelector<输入数据, key的类型>
                .keyBy(new KeySelector<ClickEvent, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(ClickEvent event) throws Exception {
                        return Tuple2.of(event.username, event.url);
                    }
                })
                .process(new UrlCountStatistic())
                .print();
        env.execute();
    }

    private static class UrlCountStatistic extends KeyedProcessFunction<Tuple2<String, String>, ClickEvent, Tuple2<Tuple2<String, String>, Integer>> {

        //声明值变量累加器--代表url访问次数
        private ValueState<Integer> counts;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化值变量
            counts = getRuntimeContext().getState(new ValueStateDescriptor<Integer>(
                    "COUNTS",
                    Types.INT
            ));
        }

        // 输出结果类似于 ((Alice, ./buy), 1)
        @Override
        public void processElement(ClickEvent in, Context ctx, Collector<Tuple2<Tuple2<String, String>, Integer>> out) throws Exception {
            if (counts.value() == null) {
                counts.update(1);
            } else {
                counts.update(1 + counts.value());
            }
            out.collect(Tuple2.of(Tuple2.of(in.username, in.url), counts.value()));
        }
    }


}
