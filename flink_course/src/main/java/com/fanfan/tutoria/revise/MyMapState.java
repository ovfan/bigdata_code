package com.fanfan.tutoria.revise;

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
 * @ClassName: MyMapState
 * @Description: TODO 字典状态变量 练习
 * @Author: fanfan
 * @DateTime: 2022年08月25日 13时37分
 * @Version: v1.0
 */
public class MyMapState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 对用户访问url进行统计
        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                .process(new UrlCount())
                .print();

        env.execute();
    }

    public static class UrlCount extends KeyedProcessFunction<String, ClickEvent, String> {
        private MapState<String, Integer> urlcount;

        @Override
        public void open(Configuration parameters) throws Exception {
            urlcount = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>(
                    "url-count",
                    Types.STRING,
                    Types.INT
            ));
        }

        @Override
        public void processElement(ClickEvent in, Context ctx, Collector<String> out) throws Exception {
            if (urlcount.get(in.url) == null) {
                urlcount.put(in.url, 1);
            } else {
                urlcount.put(in.url, urlcount.get(in.url) + 1);
            }
            StringBuilder res = new StringBuilder();

            res.append("{\n用户:" + ctx.getCurrentKey() + "\n" + "\t{");

            for (String url : urlcount.keys()) {
                res.append("\t\t" + url + " : " + urlcount.get(url) + "\n");
            }
            res.append("\t}\n}");

            out.collect(res.toString());
        }
    }
}
