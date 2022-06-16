package com.fanfan.flink.utils;

/**
 * @ClassName: ClickEvent
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月15日 15时24分
 * @Version: v1.0
 *  数据源类，点击事件对象
 *  属性包括：用户名-点击的url-点击的时间
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
        return "(" + "username='" + username +  ", url='" + url  + ", ts=" + ts + ")";
    }
}
