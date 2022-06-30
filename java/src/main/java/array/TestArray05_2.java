package array;

import java.util.Scanner;

/**
 * @ClassName: TestArray05_2
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月30日 16时25分
 * @Version: v1.0
 * TODO 二分查找练习
 */
public class TestArray05_2 {
    public static void main(String[] args) {
        // 数组的静态初始化
        int[] array = new int[]{13, 23, 24, 35, 46, 67, 74, 78, 98};

        System.out.print("请输入你要查找的数字: ");
        // 获取系统输入
        Scanner sc = new Scanner(System.in);

        // 要查找的目标值
        int target = sc.nextInt();

        int flag = 0; // 0代表未找到，1代表找到
        for (int left = 0, right = array.length - 1; left <= right; ) {
            int middle = left + (right - left) / 2;
            if (target == array[middle]) {
                flag = 1;
                break;
            } else if (target < array[middle]) {
                right = middle - 1;
            } else {
                left = middle + 1;
            }
            middle = left + (right - left) / 2;
        }
        if (flag == 1) {
            System.out.println("查找到了值为:" + target);
        } else {
            System.out.println("未找到");
        }

        // 释放sc
        sc.close();
    }
}
