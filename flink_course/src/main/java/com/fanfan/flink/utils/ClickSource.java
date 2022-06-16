package com.fanfan.flink.utils;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @ClassName: ClickSource
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月15日 15时32分
 * @Version: v1.0
 */

// TODO --自定义数据源,需要实现SourceFunction<E>
public class ClickSource implements SourceFunction<ClickEvent> {
    // 持续不断发送数据
    private boolean runnig = true;

    // 模拟生成数据
    private Random random = new Random();
    private String[] userArray = {"fanfan", "qiqi", "peipei"};
    private String[] urlArray = {"./home", "./cart", "./other", "./phone"};

    @Override
    public void run(SourceContext<ClickEvent> ctx) throws Exception {
        while (runnig) {
            // 发送的数据对象为 一个一个的点击事件
            ctx.collect(new ClickEvent(
                    userArray[random.nextInt(userArray.length)],    //用户名
                    urlArray[random.nextInt(urlArray.length)],      //点击的页面
                    Calendar.getInstance().getTimeInMillis()        //点击页面的事件事件
            ));

            // 每秒钟发送一次数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        runnig = false;
    }
}
