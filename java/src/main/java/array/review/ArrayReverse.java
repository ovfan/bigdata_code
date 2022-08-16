package array.review;

/**
 * @ClassName: ArrayReverse
 * @Description: TODO 实现数组元素的反转
 * @Author: fanfan
 * @DateTime: 2022年08月16日 22时08分
 * @Version: v1.0
 */
public class ArrayReverse {
    public static void main(String[] args) {
        // 定义一个数组
        int[] arr = {7, 5, 3, 2, 1};

        // 方式1验证
        int[] newArr = reverse(arr);
        for (int i = 0; i < newArr.length; i++) {
            System.out.print(newArr[i] + " ");
        }
        System.out.println();
        // 方式2验证
        int[] newArr2 = reverse2(arr);
        for (int i = 0; i < newArr2.length; i++) {
            System.out.print(newArr2[i] + " ");
        }
    }

    // 方式1. 使用新数组
    public static int[] reverse(int[] arr) {
        int[] newArr = new int[arr.length];
        for (int i = 0; i < arr.length; i++) {
            newArr[i] = arr[arr.length - 1 - i];
        }
        return newArr;
    }

    // 方式2. 数组元素 首尾交换
    public static int[] reverse2(int[] arr) {
        for (int i = 0; i < arr.length / 2; i++) {
            int temp = arr[i];
            arr[i] = arr[arr.length - 1 - i];
            arr[arr.length - 1 - i] = temp;
        }
        return arr;
    }
}
