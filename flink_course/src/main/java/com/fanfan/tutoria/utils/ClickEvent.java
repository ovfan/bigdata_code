package com.fanfan.tutoria.utils;

import java.sql.Time;
import java.sql.Timestamp;

/**
 * @ClassName: ClickEvent
 * @Description: POJO类-ClickEvent
 * @Author: fanfan
 * @DateTime: 2022年08月19日 12时06分
 * @Version: v1.0
 */

/**
 * 点击流事件
 */
// POJO类要求：
// 1.必须是公有类
public class ClickEvent {

    // 2.所有字段必须是公有字段
    public String username;
    public String url;
    public Long ts;

    // 3. 必须有空参构造器
    public ClickEvent() {
    }

    public ClickEvent(String username, String url, Long ts) {
        this.username = username;
        this.url = url;
        this.ts = ts;
    }

    @Override
    public String toString() {
        // new TimeStamp(ts) 将时间戳转换为 北京时间的形式
        return "点击事件 {" +
                "用户名 ='" + username +
                ", 网页 ='" + url +
                ", 时间戳 =" + new Timestamp(ts) + '}';
    }
}
