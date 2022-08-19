package com.fanfan.flink.utils;

import java.sql.Timestamp;

/**
 * @ClassName: UserViewCountPerWindow
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月21日 16时51分
 * @Version: v1.0
 * TODO 用户访问统计的类(用户名，统计次数，开始时间，结束时间)
 */
public class UserViewCountPerWindow {
    public String username;
    public Long count;
    public Long windowStartTime;
    public Long windowEndTime;

    public UserViewCountPerWindow() {
    }

    public UserViewCountPerWindow(String username, Long count, Long windowStartTime, Long windowEndTime) {
        this.username = username;
        this.count = count;
        this.windowStartTime = windowStartTime;
        this.windowEndTime = windowEndTime;
    }

    @Override
    public String toString() {
        return "(" +
                "" + username + '\'' +
                ", " + count +
                ", " + new Timestamp(windowStartTime) +
                "~" + new Timestamp(windowEndTime) +
                ')';
    }
}
