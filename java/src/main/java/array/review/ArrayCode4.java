package array.review;

import java.util.Scanner;

/**
 * @ClassName: ArrayCode4
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月16日 19时24分
 * @Version: v1.0
 */
public class ArrayCode4 {
    public static void main(String[] args) {
        int[] arr = new int[26];
        computeCount(arr);
    }
    public static void computeCount(int[] arr){
        Scanner sc = new Scanner(System.in);
        boolean flag = true;
        while(flag){
            char ch = sc.next().charAt(0);
            if(ch >= 'a' && ch <= 'z'){
                arr[ch - 97] = ++arr[ch - 97];
            }else if (ch == '0'){
                flag = false;
            }

        }
        for (int i = 0; i < arr.length; i++) {
            System.out.println("字符:"+(char)(i+'a') + "出现的次数为:" + arr[i]);
        }

    }

}
