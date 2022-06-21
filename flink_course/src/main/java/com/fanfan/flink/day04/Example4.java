package com.fanfan.flink.day04;

import com.fanfan.flink.utils.ClickEvent;
import com.fanfan.flink.utils.ClickSource;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collection;

/**
 * @ClassName: Example4
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月21日 11时45分
 * @Version: v1.0
 * TODO 使用字典状态变量完成 url访问次数统计
 */
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                // 添加数据源
                .addSource(new ClickSource())
                // 根据用户名分组
                .keyBy(r -> r.username)
                .process(new MyUrlCount())
                .print();
        env.execute();
    }

    public static class MyUrlCount extends KeyedProcessFunction<String, ClickEvent, String> {
        //声明字典状态变量
        private MapState<String, Integer> urlCount;

        @Override
        public void open(Configuration parameters) throws Exception {
            urlCount = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                    "urlCount",
                    Types.STRING,
                    Types.INT
            ));
        }

        @Override
        public void processElement(ClickEvent in, Context ctx, Collector<String> out) throws Exception {
            // urlCount.contains检查的是in.username对应的字典状态变量中是否
            // 包含in.url这个key
            if (!urlCount.contains(in.url)) {
                // 用户in.username第一次访问in.url
                // 写入的是in.username对应的字典状态变量
                urlCount.put(in.url, 1);
            } else {
                // 访问量加 1
                Integer oldCount = urlCount.get(in.url);
                Integer newCount = ++oldCount;
                urlCount.put(in.url, newCount);
            }

            //格式化输出
            StringBuilder resultTmp = new StringBuilder();
            resultTmp.append(ctx.getCurrentKey() + "{\n");
            //遍历key输出访问次数
            for (String key : urlCount.keys()) {
                resultTmp.append(key + " -> " + urlCount.get(key) + "\n");
            }
            resultTmp.append("}\n");

//            StringBuilder result = new StringBuilder();
//            result.append(ctx.getCurrentKey() + " {\n");
//            // 遍历in.username对应的字典状态变量的所有的key
//            for (String key : urlCount.keys()) {
//                result.append("  ")
//                        .append("\"" + key + "\" -> ")
//                        .append(urlCount.get(key) + ",\n");
//            }
//            result.append("}\n");

            out.collect(resultTmp.toString());
        }
    }
}
