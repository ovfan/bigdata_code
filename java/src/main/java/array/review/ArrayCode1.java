package array.review;

import java.util.Scanner;

/**
 * @ClassName: ArrayCode1
 * @Description: TODO 复习数组的定义及 完成一个日期判断的案例
 * @Author: fanfan
 * @DateTime: 2022年08月16日 18时29分
 * @Version: v1.0
 */
public class ArrayCode1 {
    public static void main(String[] args) {
        // 1. 数组的定义分为两种，静态初始化，动态初始化
        // 1.1 初始化的目的 确定元素的类型 及 数组元素的个数
        // 1.2 静态初始化
        int[] arr = new int[]{1,2,3,4};
        int[] arr1 = {2,3,4,5};

        // 1.3 动态初始化
        int[] arr2 = new int[4];
        arr2[0] = 6;
        arr[1] = 7;

        System.out.println("arr2.toString() = " + arr2.toString());
        System.out.println("arr = " + arr[3]);

        computeDay();

    }
    public static void computeDay(){
        System.out.println("请输入数字:");
        // 1. 获取键盘输入的数字
        Scanner sc = new Scanner(System.in);
        int number = sc.nextInt();

        // 2. 定义存储同种日期类型的数组
        String[] workdays = {"星期一","星期二","星期三","星期四","星期五","星期六","星期日"};

        switch (number){
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                System.out.println(workdays[number-1]);
                break;
            default:
                System.out.println("输入有误");
                break;
        }
        sc.close();
    }
}
