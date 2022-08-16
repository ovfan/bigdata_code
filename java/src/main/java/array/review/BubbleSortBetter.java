package array.review;

/**
 * @ClassName: BubbleSortBetter
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月16日 22时29分
 * @Version: v1.0
 */
public class BubbleSortBetter {
    public static void main(String[] args) {
        int[] arr = {1, 3, 5, 9, 7};
        int lun = 0;
        for (int i = 1; i < arr.length; i++) {
            lun++;
            boolean flag = true; // 假设数组已经是有序的
            for (int j = 0; j < arr.length - i; j++) {
                if (arr[j] > arr[j + 1]) {
                    int temp = arr[j];
                    arr[j] = arr[j + 1];
                    arr[j + 1] = temp;
                    flag = false; // 说明此时数组还未排好序
                }
            }
            if (flag) {
                break;
            }
        }

        // 结果验证:
        for (int i = 0; i < arr.length; i++) {
            System.out.print(arr[i] + "\t");
        }
    }
}
