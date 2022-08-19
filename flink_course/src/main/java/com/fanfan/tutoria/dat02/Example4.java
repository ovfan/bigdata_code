package com.fanfan.tutoria.dat02;

import com.fanfan.tutoria.utils.ClickEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example4
 * @Description: TODO 实现过滤 复制 功能
 * @Author: fanfan
 * @DateTime: 2022年08月19日 14时29分
 * @Version: v1.0
 */
public class Example4 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置全局并行度
        env.setParallelism(1);

        env
                .fromElements("white","black","blue")
                .flatMap(new MyFlatMap())
                .print("使用外部类 实现flatMap");

        env
                .fromElements("white","black","blue")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String in, Collector<String> out) throws Exception {
                        if(in.equals("white")){
                            out.collect(in);
                        }else if(in.equals("black")){
                            out.collect(in);
                            out.collect(in);
                        }
                    }
                })
                .print("使用匿名内部类方式 实现flatMap");

        env
                .fromElements("white","black","blue")
                .flatMap((String in,Collector<String> out) ->{
                    if(in.equals("white")){
                        out.collect(in);
                    }else if(in.equals("black")){
                        out.collect(in);
                        out.collect(in);
                    }
                })
                // lambda 表达式在使用时需要注意:
                //      collector<String> 类型擦除成 -> collector<Object>
                //      需要对输出数据的类型做注解
                .returns(Types.STRING)
                .print("使用lambda 匿名函数方式 实现flatMap");

        env.execute();
    }
    public static class MyFlatMap implements FlatMapFunction<String,String>{
        @Override
        public void flatMap(String in, Collector<String> out) throws Exception {
            if(in.equals("white")){
                out.collect(in);
            }else if(in.equals("black")){
                out.collect(in);
                out.collect(in);
            }
        }
    }
}
