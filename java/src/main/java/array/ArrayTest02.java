package array;

/**
 * @ClassName: ArrayTest02
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月10日 22时07分
 * @Version: v1.0
 */
public class ArrayTest02 {
    public static void main(String[] args) {
        int[] arr = {1, 2, 3};
        System.out.println(arr);            // [I@1b6d3586
        System.out.println(arr.hashCode()); // 十进制：460141958 十六进制：1B6D3586
        System.out.println(arr.toString()); // [I@1b6d3586

        // TODO 细节：
        //  1. [I@1b6d3586：[表示一维数组，I表示元素是int类型，@后面的1b6d3586是数组对象的hashCode值的十六进制值。
        //  2.  hashCode,只有当对象存储到 哈希表等哈希结构的容器中才会有用
    }
}
