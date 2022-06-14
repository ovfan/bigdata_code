package collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Predicate;

/**
 * @ClassName: CollTest2
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月14日 21时10分
 * @Version: v1.0
 *  理解迭代器的概念:所谓的迭代器就是用来遍历集合中元素的 工具 / 对象
 *  Collection系列的集合中，有一个方法：Iterator iterator();  这个方法是返回遍历当前集合对象的一个迭代器对象
 */
public class CollTest2 {
    public static void main(String[] args) {
        // Collection接口 是集合层次中的根接口
        Collection coll = new ArrayList();
        // 添加元素
        coll.add("hi");
        coll.add("hello");
        coll.add("hello1");
        coll.add("hello2");
        coll.add("hello3");

        // 遍历集合
        // 1. 迭代器
        Iterator iterator = coll.iterator();
        while(iterator.hasNext()){
            Object item = iterator.next();
            System.out.println(item);
        }
        Collection coll2 = new ArrayList();
        // 方式2，foreach，底层依然为iterator
//        for (Object o : coll) {
//            // 需求：删除集合元素中含有hello的数据
//            String str = o.toString();
//            if(str.contains("hello")){
//                coll2.add(str);
//            }
//        }
//        coll.removeAll(coll2);
//        System.out.println(coll);

        // 另一种写法
        // 需求：删除集合元素中含有hello的数据
        coll.removeIf(new Predicate() {
            @Override
            public boolean test(Object o) {
                return o.toString().contains("hello");
            }
        });
        System.out.println(coll);
    }
}
