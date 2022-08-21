package com.fanfan.tutoria.day04;

import com.fanfan.tutoria.utils.ClickEvent;
import com.fanfan.tutoria.utils.ClickSource;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @ClassName: Example3
 * @Description: TODO 练习字典状态变量--实现用户访问 url的次数统计
 * @Author: fanfan
 * @DateTime: 2022年08月21日 11时38分
 * @Version: v1.0
 */
public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                .process(new UserUrlCount())
                .print();


        env.execute();
    }

    public static class UserUrlCount extends KeyedProcessFunction<String, ClickEvent, String> {
        // 声明字典状态变量
        private MapState<String, Integer> urlCount;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化字典状态变量
            urlCount = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                    "url-count",
                    Types.STRING,
                    Types.INT
            ));
        }

        @Override
        public void processElement(ClickEvent in, Context ctx, Collector<String> out) throws Exception {
            StringBuilder result = new StringBuilder();
            if (urlCount.get(in.url) == null) {
                urlCount.put(in.url, 1);
            } else {
                urlCount.put(in.url, urlCount.get(in.url) + 1);
            }
            result.append(" { \n" + ctx.getCurrentKey() + "\n");
            for (String key : urlCount.keys()) {
                result.append("\t { " + key + " : " + urlCount.get(key) + " }\n");
            }
            result.append("}");

            out.collect(result.toString());
        }
    }
}
