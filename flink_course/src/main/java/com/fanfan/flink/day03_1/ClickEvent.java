package com.fanfan.flink.day03_1;

import java.sql.Timestamp;

/**
 * @ClassName: ClickEvent
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月05日 14时51分
 * @Version: v1.0
 */
// TODO 定义事件序列--POJO类
public class ClickEvent {
    // TODO 点击事件
    public String user;
    public String url;
    public Long ts; // 事件时间

    public ClickEvent() {
    }

    public ClickEvent(String user, String url, Long ts) {
        this.user = user;
        this.url = url;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "{" + user + " ," + url + ", " + new Timestamp(ts) + "}";
    }
}
