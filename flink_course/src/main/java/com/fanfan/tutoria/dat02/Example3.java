package com.fanfan.tutoria.dat02;

import com.fanfan.tutoria.utils.ClickEvent;
import com.fanfan.tutoria.utils.ClickSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example3
 * @Description: 过滤出 mary的点击流
 * @Author: fanfan
 * @DateTime: 2022年08月19日 14时29分
 * @Version: v1.0
 */
public class Example3 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置全局并行度
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .filter(new FilterFunction<ClickEvent>() {
                    @Override
                    public boolean filter(ClickEvent in) throws Exception {
                        return in.username.equals("mary");
                    }
                })
                .print("使用匿名类的方式 实现map");

        env
                .addSource(new ClickSource())
                .filter(r ->r.username.equals("mary"))
                .print("使用匿名函数的方式 实现map");

        env
                .addSource(new ClickSource())
                .filter(new MyFilter())
                .print("使用外部类的方式 实现map");

        env
                .addSource(new ClickSource())
                .flatMap((ClickEvent in,Collector<ClickEvent> out) ->{
                    if(in.username.equals("mary")){
                        out.collect(in);
                    }
                })
                .returns(Types.POJO(ClickEvent.class))
                .print("使用flatMap方式 实现map");
        env.execute();
    }
    public static class MyFilter implements FilterFunction<ClickEvent>{
        @Override
        public boolean filter(ClickEvent in) throws Exception {
            return in.username.equals("mary");
        }
    }
}
