package com.fanfan.tutoria.revise;

/**
 * @ClassName: Intstatistic
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月25日 12时32分
 * @Version: v1.0
 */
public class Intstatistic {
    public Integer min;
    public Integer max;
    public Integer sum;
    public Integer count;
    public Integer avg;

    public Intstatistic() {
    }

    public Intstatistic(Integer min, Integer max, Integer sum, Integer count, Integer avg) {
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.count = count;
        this.avg = avg;
    }

    @Override
    public String toString() {
        return "Intstatistic{" +
                "min=" + min +
                ", max=" + max +
                ", sum=" + sum +
                ", count=" + count +
                ", avg=" + avg +
                '}';
    }
}
