package array.review;

/**
 * /*
 * 目标：从小到大
 * 直接选择排序：
 * 每一轮从当前未排序的元素中找到“最小值”，然后用这个最小值与它应该在的位置元素交换。
 * <p>
 * 第一轮：{6,9（前）,2,9（后）,1}
 * 第一轮所有元素都是未排序元素。
 * 找出本轮的最小值 1，它现在在arr[4]，它应该在arr[0]， arr[4]与arr[0]    {1,9（前）,2,9（后）,6}
 * 第二轮：{1,9,2,9,6}
 * 第二轮未排序元素    {9,2,9,6}
 * 找出本轮的最小值 2，它现在在arr[2]， 它应该在arr[1]， arr[2]与arr[1]  {1,2,9（前）,9（后）,6}
 * <p>
 * 第三轮：  {1,2,9（前）,9（后）,6}
 * 第三轮未排序元素    {9（前）,9（后）,6}
 * 找出本轮的最小值，6，它现在在arr[4]，它应该在arr[2]，arr[4]与arr[2]   {1,2,6,9（后）,9（前）}
 * <p>
 * 第四轮： {1,2,6,9（后）,9（前）}
 * 第四轮未排序元素  {9（后）,9（前）}
 * 找出本轮的最小值，9（后）,它现在在arr[3]，它应该在arr[3]，不用换
 */

public class SelectSort {
    public static void main(String[] args) {
        int[] arr = {6, 9, 2, 9, 1};

        /*
        arr.length=5
        i=0,1,2,3
         */
        for (int i = 0; i < arr.length - 1; i++) {
            /*
            第1轮，最小值正确的位置arr[0]
            第2轮，最小值正确的位置arr[1]
            第3轮，最小值正确的位置arr[2]
            第4轮，最小值正确的位置arr[3]
            每一轮最小值正确的位置就是arr[i]

            第1轮，最小值查找范围arr[0]~arr[4]
            第2轮，最小值查找范围arr[1]~arr[4]
            第3轮，最小值查找范围arr[2]~arr[4]
            第4轮，最小值查找范围arr[3]~arr[4]
             */
            int min = arr[i]; //假设本轮要找最小值的所有元素的第一个元素最小
            int index = i;
            for (int j = i + 1; j < arr.length; j++) {
                if (arr[j] < min) {
                    min = arr[j];
                    index = j;
                }
            }
            //找出本轮最小值现在的位置arr[index]，它应该在arr[i]
            if (index != i) {
                int temp = arr[i];
                arr[i] = arr[index];
                arr[index] = temp;
            }
        }
        //显示结果
        for (int i = 0; i < arr.length; i++) {
            System.out.println(arr[i]);
        }
    }
}
