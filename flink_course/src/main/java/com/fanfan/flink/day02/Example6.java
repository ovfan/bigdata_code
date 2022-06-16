package com.fanfan.flink.day02;

import com.fanfan.flink.utils.ClickEvent;
import com.fanfan.flink.utils.ClickSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example6
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月16日 19时21分
 * @Version: v1.0
 */
public class Example6 {
    public static void main(String[] args) throws Exception {
        // TODO 使用Filter过滤出来用户是 fanfan的点击事件对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .filter(r -> r.username.equals("fanfan"))
                .print("使用匿名函数的方式实现filter");

        env
                .addSource(new ClickSource())
                .filter(new FilterFunction<ClickEvent>() {
                    @Override
                    public boolean filter(ClickEvent in) throws Exception {
                        return in.username.equals("fanfan");
                    }
                })
                .print("使用匿名内部类的方式实现filter");

        env
                .addSource(new ClickSource())
                .filter(new MyFilter())
                .print("使用外部类的方式实现filter");

        env
                .addSource(new ClickSource())
                .flatMap(new FlatMapFunction<ClickEvent, ClickEvent>() {
                    @Override
                    public void flatMap(ClickEvent in, Collector<ClickEvent> out) throws Exception {
                        if(in.username.equals("fanfan")){
                            out.collect(in);
                        }
                    }
                })
                .print("使用flatMap实现filter功能");
        env.execute();
    }

    public static class MyFilter implements FilterFunction<ClickEvent> {
        @Override
        public boolean filter(ClickEvent in) throws Exception {
            return in.username.equals("fanfan");
        }
    }
}
