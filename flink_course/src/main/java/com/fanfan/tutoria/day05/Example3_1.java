package com.fanfan.tutoria.day05;

import com.fanfan.tutoria.utils.ClickEvent;
import com.fanfan.tutoria.utils.ClickSource;
import com.fanfan.tutoria.utils.UserViewCountPerWindow;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: Example2
 * @Description: 使用keyedProcessFunction实现滚动时间窗口统计
 * @Author: fanfan
 * @DateTime: 2022年08月23日 23时39分
 * @Version: v1.0
 */
public class Example3_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1. 读取数据
        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                .process(new MyTumblingProcessingTimeWindowFunction(5000L))
                .print();

        env.execute();
    }

    public static class MyTumblingProcessingTimeWindowFunction extends KeyedProcessFunction<String, ClickEvent, UserViewCountPerWindow> {
        // 窗口大小
        private Long windowSize;
        private Long acc;

        public MyTumblingProcessingTimeWindowFunction(Long windowSize) {
            this.windowSize = windowSize;
        }

        private MapState<Tuple2<Long, Long>, Long> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化值状态变量
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Tuple2<Long, Long>, Long>(
                    "window-info",
                    Types.TUPLE(Types.LONG, Types.LONG),
                    Types.LONG)
            );
        }

        @Override
        public void processElement(ClickEvent in, Context ctx, Collector<UserViewCountPerWindow> out) throws Exception {
            // 计算窗口的开始时间 和 结束时间
            long currTs = ctx.timerService().currentProcessingTime();
            long windowStartTime = currTs - currTs % windowSize;
            long windowEndTime = windowStartTime + windowSize;
            // 将窗口信息(开始时间，结束时间)包装进元组中
            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartTime, windowEndTime);
            // 将数据写入窗口中
            if (!mapState.contains(windowInfo)) {
                // 第一条数据写入窗口中
                mapState.put(windowInfo, 1L);
            } else {
                // 输入数据所属窗口已经存在，那么直接将输入数据添加到窗口中
                acc = mapState.get(windowInfo) + 1L;
            }

            // 注册定时器
            ctx.timerService().registerProcessingTimeTimer(windowEndTime - 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<UserViewCountPerWindow> out) throws Exception {
            // 还原窗口信息
            long windowEndTime = timestamp + 1L;
            long windowStartTime = windowEndTime - windowSize;
            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartTime, windowEndTime);

            long count = mapState.get(windowInfo);
            out.collect(new UserViewCountPerWindow(
                    ctx.getCurrentKey(),
                    count,
                    windowStartTime,
                    windowEndTime

            ));
        }
    }

}
