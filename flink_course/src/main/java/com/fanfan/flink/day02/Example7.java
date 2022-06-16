package com.fanfan.flink.day02;

import com.fanfan.flink.utils.ClickSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.lang.reflect.Type;

/**
 * @ClassName: Example7
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月16日 19时30分
 * @Version: v1.0
 */
public class Example7 {
    public static void main(String[] args) throws Exception {
        //TODO 使用fllatMap实现事件对象的过滤，保留与复制
        // 数据是white 保留，是 blue复制一份，是yellow过滤掉

        // 获取flink流的执行环境对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements("white", "blue", "yello")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String in, Collector<String> out) throws Exception {
                        if (in.equals("white")) {
                            out.collect(in);
                        } else if (in.equals("blue")) {
                            out.collect(in);
                            out.collect(in);
                        }
                    }
                })
                .print("使用匿名内部类的方式实现flatMap");

        env
                .fromElements("white", "blue", "yello")
                .flatMap(new MyFlatMap())
                .print("使用外部类的方式实现流对象的操作");

        //TODO 使用匿名函数的方式实现flatMap
        env
                .fromElements("white", "blue", "yello")
                .flatMap((String in, Collector<String> out) -> {
                    if (in.equals("white")) {
                        out.collect(in);
                    } else if (in.equals("blue")) {
                        out.collect(in);
                        out.collect(in);
                    }
                })
                // 在编译器发生了类型擦除
                // Collector<String> -> Collector<Object>
                // 所以需要对flatMap输出数据的类型做类型注解
                .returns(Types.STRING)
                .print("使用匿名函数的方式实现flatMap");
        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<String, String> {

        @Override
        public void flatMap(String in, Collector<String> out) throws Exception {
            if (in.equals("white")) {
                out.collect(in);
            } else if (in.equals("blue")) {
                out.collect(in);
                out.collect(in);
            }
        }
    }
}
