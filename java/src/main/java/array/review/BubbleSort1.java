package array.review;

/**
 * @ClassName: BubbleSort1
 * @Description: TODO 实现冒泡排序
 * @Author: fanfan
 * @DateTime: 2022年08月16日 20时23分
 * @Version: v1.0
 */
public class BubbleSort1 {
    public static void main(String[] args) {
        int[] arr = {1, 21, 8, 9, 7, 9, 15, 18};
        for (int i = 1; i < arr.length; i++) {
            for (int j = 0; j < arr.length - i; j++) {
                if (arr[j] > arr[j + 1]) {
                    int temp = arr[j];
                    arr[j] = arr[j + 1];
                    arr[j + 1] = temp;
                }
            }
        }

        for (int i = 0; i < arr.length; i++) {
            System.out.print(arr[i] + " ");
        }
    }
}
