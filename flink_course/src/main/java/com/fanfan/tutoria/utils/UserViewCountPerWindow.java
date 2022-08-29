package com.fanfan.tutoria.utils;

import java.sql.Time;

/**
 * @ClassName: UserViewCountPerWindow
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月21日 16时47分
 * @Version: v1.0
 */
public class UserViewCountPerWindow {
    public String username;      // 用户名
    public Long count;           // 访问次数
    public Long WindowStartTime; // 窗口开始时间
    public Long WindowEndTime;   // 窗口结束时间

    public UserViewCountPerWindow() {
    }

    public UserViewCountPerWindow(String username, Long count, Long windowStartTime, Long windowEndTime) {
        this.username = username;
        this.count = count;
        WindowStartTime = windowStartTime;
        WindowEndTime = windowEndTime;
    }

    @Override
    public String toString() {
        return "UserViewCountPerWindow{" +
                "username='" + username + '\'' +
                ", count=" + count +
                ", WindowStartTime=" + new Time(WindowStartTime) +
                ", WindowEndTime=" + new Time(WindowEndTime) +
                '}';
    }
}
