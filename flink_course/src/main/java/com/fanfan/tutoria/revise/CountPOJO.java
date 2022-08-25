package com.fanfan.tutoria.revise;

/**
 * @ClassName: CountPOJO
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月25日 09时23分
 * @Version: v1.0
 */
public class CountPOJO {
    public String word;
    public Long count;

    public CountPOJO() {
    }

    public CountPOJO(String word, Long count) {
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return "(" +
                "word='" + word + '\'' +
                ", count=" + count +
                ')';
    }
}
