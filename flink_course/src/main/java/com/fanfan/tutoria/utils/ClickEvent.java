package com.fanfan.tutoria.utils;

import java.sql.Time;
import java.sql.Timestamp;

/**
 * @ClassName: ClickEvent
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月19日 12时06分
 * @Version: v1.0
 */
public class ClickEvent {
    public String username;
    public String url;
    public Long ts;

    public ClickEvent() {
    }

    public ClickEvent(String username, String url, Long ts) {
        this.username = username;
        this.url = url;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "点击事件 {" +
                "用户名 ='" + username +
                ", 网页 ='" + url +
                ", 时间戳 =" + new Timestamp(ts) + '}';
    }
}
