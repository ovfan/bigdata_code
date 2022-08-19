package com.fanfan.tutoria.utils;

/**
 * @ClassName: IntStatistic
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月19日 16时58分
 * @Version: v1.0
 */
public class IntPOJO {
    public Integer min;
    public Integer max;
    public Integer sum;
    public Integer count;
    public Integer avg;

    public IntPOJO() {
    }

    public IntPOJO(Integer min, Integer max, Integer sum, Integer count, Integer avg) {
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.count = count;
        this.avg = avg;
    }

    @Override
    public String toString() {
        return "(" +
                "min=" + min +
                ", max=" + max +
                ", sum=" + sum +
                ", count=" + count +
                ", avg=" + avg +
                ')';
    }
}
