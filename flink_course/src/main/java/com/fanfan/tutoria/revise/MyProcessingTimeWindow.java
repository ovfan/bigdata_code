package com.fanfan.tutoria.revise;

import com.fanfan.tutoria.utils.ClickEvent;
import com.fanfan.tutoria.utils.ClickSource;
import com.fanfan.tutoria.utils.UserViewCountPerWindow;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: MyProcessingTimeWindow
 * @Description: TODO 使用KeyedProcessFunction实现滚动时间窗口 完成 用户每5s钟访问url的次数统计
 * @Author: fanfan
 * @DateTime: 2022年08月25日 19时02分
 * @Version: v1.0
 */
public class MyProcessingTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .keyBy(new KeySelector<ClickEvent, String>() {
                    @Override
                    public String getKey(ClickEvent in) throws Exception {
                        return in.username;
                    }
                })
                .process(new MyTumblingProcessingTimeWindow(5000L))
                .print();

        env.execute();
    }

    public static class MyTumblingProcessingTimeWindow extends KeyedProcessFunction<String, ClickEvent, UserViewCountPerWindow> {
        public Long windowSize;

        public MyTumblingProcessingTimeWindow(Long windowSize) {
            this.windowSize = windowSize;
        }

        private MapState<Tuple2<Long, Long>, List<ClickEvent>> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Tuple2<Long, Long>, List<ClickEvent>>(
                    "window-info-events",
                    Types.TUPLE(Types.LONG, Types.LONG),
                    Types.LIST(Types.POJO(ClickEvent.class))
            ));
        }

        @Override
        public void processElement(ClickEvent in, Context ctx, Collector<UserViewCountPerWindow> out) throws Exception {
            // 计算窗口信息
            long currTs = ctx.timerService().currentProcessingTime();
            long windowStartTime = currTs - currTs % windowSize;
            long windowEndTime = windowStartTime + windowSize;

            // 窗口信息 元组
            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartTime, windowEndTime);
            // 将数据写入到所属窗口中
            if (!mapState.contains(windowInfo)) {
                ArrayList<ClickEvent> events = new ArrayList<>();
                events.add(in);
                mapState.put(windowInfo, events);
            } else {
                mapState.get(windowInfo).add(in);
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
            String username = ctx.getCurrentKey();
            long count = mapState.get(windowInfo).size();
            out.collect(new UserViewCountPerWindow(
                    username,
                    count,
                    windowStartTime,
                    windowEndTime
            ));

            // 不要忘记销毁窗口
            mapState.remove(windowInfo);
        }

    }
}
