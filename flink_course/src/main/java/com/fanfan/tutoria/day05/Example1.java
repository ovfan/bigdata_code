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
 * @ClassName: Example1
 * @Description: TODO 使用KeyedProcessFunction 完成 ProcessWindowFcunction 的底层实现
 * @Author: fanfan
 * @DateTime: 2022年08月23日 09时00分
 * @Version: v1.0
 */
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                .process(new MyTumblingProcessingTimeWindowFunction(5000L))
                .print();

        env.execute();
    }

    public static class MyTumblingProcessingTimeWindowFunction extends KeyedProcessFunction<String, ClickEvent, UserViewCountPerWindow> {
        public long windowSize; // 时间窗口大小

        public MyTumblingProcessingTimeWindowFunction(long windowSize) {
            this.windowSize = windowSize;
        }

        // 声明 字典状态变量 保存 窗口的key 和 属于该窗口中 所有的数据
        private MapState<Tuple2<Long, Long>, List<ClickEvent>> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 状态变量的初始化
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Tuple2<Long, Long>, List<ClickEvent>>(
                    "window-info",
                    Types.TUPLE(Types.LONG, Types.LONG),
                    Types.LIST(Types.POJO(ClickEvent.class))
            ));
        }

        @Override
        public void processElement(ClickEvent in, Context ctx, Collector<UserViewCountPerWindow> out) throws Exception {

            long currTs = ctx.timerService().currentProcessingTime();
            // 计算窗口的开始时间
            long windowStartTime = currTs - currTs % windowSize;
            // 计算窗口的结束时间
            long windowEndTime = windowStartTime + windowSize;
            // 窗口信息元组 -- 当成key
            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartTime, windowEndTime);

            // 将输入数据分发到所属窗口
            if (!mapState.contains(windowInfo)) {
                // 说明输入数据 是窗口第一个元素
                ArrayList<ClickEvent> events = new ArrayList<>();
                events.add(in);
                mapState.put(windowInfo, events);
            } else {
                // 输入数据所属窗口已经存在，那么直接将输入数据添加到窗口中
                mapState.get(windowInfo).add(in);
            }

            // 在窗口结束时间 -1 毫秒，注册一个定时器
            ctx.timerService().registerProcessingTimeTimer(windowEndTime - 1L);

        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx, Collector<UserViewCountPerWindow> out) throws Exception {
            // 根据ts参数计算出窗口结束时间
            long windowEndTime = ts + 1L;
            long windowStartTime = windowEndTime - windowSize;

            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartTime, windowEndTime);

            long count = mapState.get(windowInfo).size();

            out.collect(new UserViewCountPerWindow(
                    ctx.getCurrentKey(),
                    count,
                    windowStartTime,
                    windowEndTime
            ));
            mapState.remove(windowInfo);
        }
    }
}
