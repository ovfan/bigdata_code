package array;

import java.util.Scanner;

/**
 * @ClassName: ArrayTest01
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月10日 21时35分
 * @Version: v1.0
 */
public class ArrayTest01 {
    public static void main(String[] args) {
        //TODO  需求：用一个数组，保存星期一到星期天的7个英语单词，从键盘输入1-7，显示对应的单词
        // {"Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"}

        // 1. 从键盘获取输入
        Scanner input = new Scanner(System.in);

        // 静态初始化1
        // String[] weekdays = new String[]{"Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"};
        // String[] weekdays2 = {"Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"};
        // 动态初始化    --> 动态初始化注意✍的要求哦
        String[] weekdays3 = new String[7];
        weekdays3 = new String[]{"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};

        while (true) {
            System.out.print("请输入你的选择：");
            int chooseNum = input.nextInt();
            if (chooseNum >= 0 && chooseNum < 7) {
                System.out.println("今天是" + weekdays3[chooseNum]);
                break;
            } else {
                System.out.println("输入格式有误，请重写输入 1-7以内数字");
            }
        }
        // 关闭scanner资源
        input.close();
    }
}
