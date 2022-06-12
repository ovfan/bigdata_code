package array;

/**
 * @ClassName: ArrayTest06
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月13日 00时24分
 * @Version: v1.0
 */
public class ArrayTest06 {
    public static void main(String[] args) {
        // 实现二分查找算法
        int[] arr = {1, 2, 4, 5, 16, 47, 56, 67, 75, 89, 93};
        int target = 66;
        int index = -1;

        int left = 0;
        int right = arr.length - 1;
        int middle = left + (right - left) / 2;
        while (true) {
            if (left <= right) {
                if (target == arr[middle]) {
                    index = middle;
                    break;
                } else if (target > arr[middle]) {
                    left = middle + 1;
                } else {
                    right = middle - 1;
                }
                middle = left + (right - left) / 2;
            } else {
                break;
            }

        }
        if (index == -1) {
            System.out.println("未找到");
        } else {
            System.out.println("找到了 索引为" + index);
        }

    }
}
