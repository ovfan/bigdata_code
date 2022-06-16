package com.fanfan.flink.utils;

import java.sql.Timestamp;

/**
 * @ClassName: ClickEvent
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月15日 15时24分
 * @Version: v1.0
 * 发送的数据对象--POJO类
 * 定义点击事件对象(username,url,ts)
 */
public class ClickEvent {
    public String username;
    public String url;
    public Long ts; // 点击的事件时间

    public ClickEvent() {
    }

    public ClickEvent(String username, String url, Long ts) {
        this.username = username;
        this.url = url;
        this.ts = ts;
    }

    @Override
    public String toString() {
        // new Timestamp(ts) 获取本地北京时间的实例 -> 2022-06-16 18:55:58.543
        return "(" + username + ", " + url + ", " + new Timestamp(ts) + ")";
    }
}
