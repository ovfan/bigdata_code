package collection;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Predicate;

/**
 * @ClassName: CollTest1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月14日 20时43分
 * @Version: v1.0
 */
public class CollTest1 {
    public static void main(String[] args) {
        // Collection接口 是集合层次中的根接口
        Collection coll = new ArrayList();
        Collection coll2 = new ArrayList();

        // 添加元素
        coll.add("hi");
        coll.add("hello");
        coll2.add("world");
        coll2.add("spark");

        coll.addAll(coll2);
        System.out.println(coll);   // coll [hi, hello, world, spark]

        // 删除元素
        coll.remove("hi");
        coll.removeAll(coll2);
        System.out.println(coll); // ["hello"]
        coll.add("world");

        coll.removeIf(new Predicate() {
            @Override
            public boolean test(Object o) {

                String str = o.toString();
                return str.equals("world");
            }
        });
        System.out.println(coll); // ["hello"]

        // 求交集
        coll.add("hi");
        coll.add("world");
        //coll2 = [world, spark] coll = ["hello","hi","world"];
        coll.retainAll(coll2);
        System.out.println(coll);   // ["world"]
        // 清空集合
        coll.clear();
        System.out.println(coll.size());

        coll.add("hi");
        coll.add("hello");

        // 查看集合
        // 集合是否为空
        System.out.println(coll.isEmpty());
        System.out.println(coll.contains("fan"));
        System.out.println(coll.size());
        // 集合转换为数组
        Object[] array = coll.toArray();
        for (int i = 0;i<array.length -1;i++){
            System.out.println(array[i]);
        }

    }
}


