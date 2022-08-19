package com.fanfan.tutoria.dat02;

/**
 * @ClassName: Example5
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月19日 14时42分
 * @Version: v1.0
 */
public class Example5 {

    public static void main(String[] args) {
        System.out.println(add(1, 2));
        System.out.println(add("a", "b"));
    }

    public static Object add(Object x, Object y) {
        if (x instanceof Integer && y instanceof Integer) {
            return (int) x + (int) y;
        } else if (x instanceof String && y instanceof String) {
            return x.toString() + y.toString();
        }else {
            throw new RuntimeException();
        }
    }
}
