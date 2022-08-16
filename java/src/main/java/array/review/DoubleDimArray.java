package array.review;

/**
 * @ClassName: DoubleDimArray
 * @Description: TODO 二维数组的声明及初始化 (静态初始化)
 * @Author: fanfan
 * @DateTime: 2022年08月16日 23时16分
 * @Version: v1.0
 */
public class DoubleDimArray {
    public static void main(String[] args) {
        // 二维数组的静态初始化 写法1：
        int[][] arr = new int[][]{{1, 2, 3}, {2, 3, 4}, {3, 4, 5}};
        // 写法2：
        int[][] arr2 = new int[3][];
        arr2[0] = new int[]{1, 2, 3};
        arr2[1] = new int[]{2, 3, 4};
        arr[2] = new int[]{3, 4, 5};

        // 写法3:
        int[][] arr3 = {{1, 2, 3}, {2, 3, 4}, {3, 4, 5}};

        //遍历二维数组
        for (int i = 0; i < arr3.length; i++) {//外循环循环行
            //grades[i]是一个一维数组
            /*
            arr3[0]是一个一维数组{89,78,86,96,99}
            arr3[1]是一个一维数组{89,87,56,99}
            arr3[2]是一个一维数组{80,84,56,99,100,12}
             */
            for (int j = 0; j < arr3[i].length; j++) {
                System.out.print(arr3[i][j]+" ");//输出第i行的一个元素
            }
            System.out.println();//输出完i行的元素之后，换行
        }
    }
}
