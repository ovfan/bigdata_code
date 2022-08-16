package array.review;

import java.util.Random;

/**
 * @ClassName: ArrayCode3
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月16日 19时04分
 * @Version: v1.0
 */
public class ArrayCode3 {
    public static void main(String[] args) {
        int[] arr = new int[10];

        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            arr[i] = random.nextInt(100);
        }
        int count = 0;
        int evenNumber = 0; // 统计偶数单词
        int evenSum = 0;
        for (int i = 0; i < arr.length; i++) {
            count++;
            if(count % 5 == 0){
                System.out.println(arr[i]);
            }else{
                System.out.print(arr[i] + " ");
            }
            if(arr[i] % 2 ==0){
                evenSum += arr[i];
                evenNumber++;
            }
        }
        System.out.println("偶数个数为:" + evenNumber + " 偶数和为:" + evenSum);
        arraySearch(15,arr);

        getMax(arr);
    }

    // TODO 二分查找 前提是数组元素有序
    public static void arraySearch(int target,int[] arr){
        int index = -1;
        for (int i = 0; i < arr.length; i++) {
            if(target == arr[i]){
                System.out.println("target = " + target);
                index = i;
            }
        }
        if(index == -1){
            System.out.println("数组中不存在目标值 " + target);
        }
    }
    public static void getMax(int[] arr){
        int max = arr[0];
        for (int i = 1; i < arr.length; i++) {
            if(arr[i] > max){
                max = arr[i];
            }
        }
        System.out.println("max = " + max);
    }
}
