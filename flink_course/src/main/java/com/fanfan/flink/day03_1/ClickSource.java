package com.fanfan.flink.day03_1;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Random;

/**
 * @ClassName: ClickSource
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月05日 14时55分
 * @Version: v1.0
 */
//  todo 事件源
public class ClickSource implements SourceFunction<ClickEvent> {
    private boolean running = true;

    // 模拟点击信息
    private String[] userArr = {"fan","qiqi","xiaobai"};
    private String[] urlArr = {"./home","./cart","./buy"};
    private Random random = new Random();
    @Override
    public void run(SourceContext<ClickEvent> ctx) throws Exception {
        while(running){
            // 获取当前事件时间
            long currTs = Calendar.getInstance().getTimeInMillis();
            ctx.collect(new ClickEvent(
                    userArr[random.nextInt(userArr.length)],
                    urlArr[random.nextInt(urlArr.length)],
                    currTs
            ));

            Thread.sleep(1000L);
        }
    }
    @Override
    public void cancel() {
        // 取消数据发送
        running = false;
    }
}
