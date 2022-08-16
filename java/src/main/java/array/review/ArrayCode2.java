package array.review;

import java.util.Scanner;

/**
 * @ClassName: ArrayCode2
 * @Description: TODO 实现数组的二分查找
 * @Author: fanfan
 * @DateTime: 2022年08月16日 18时49分
 * @Version: v1.0
 */
public class ArrayCode2 {
    public static void main(String[] args) {
        int[] arr = {1,2,34,32,36,43,56,78,79,84,86,89};

        Scanner sc = new Scanner(System.in);
        System.out.println("请输入你要查找的目标值:" );
        int target = sc.nextInt();
        ArrayBinarySearch(target,arr);

        sc.close();

    }
    public static void ArrayBinarySearch(int target,int[] arr){
        int index = -1;
        // 1. 定义查找的边界索引
        for (int left = 0,right = arr.length-1; left <= right;){
            int middle = left + (right-left)/2;
            if(target == arr[middle]){
                System.out.println("查找的目标值已找到,索引为" + middle);
                index = middle;
                break;
            }else if(target < arr[middle]){
                right = middle -1;
                middle = left + (right-left)/2;
            }else if(target > arr[middle]){
                left = middle + 1;
                middle = left + (right-left)/2;
            }
        }
        if(index == -1){
            System.out.println("未找到");
        }
    }
}
