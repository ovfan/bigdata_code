package collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.function.Predicate;

/**
 * @ClassName: CollTest3
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月14日 21时21分
 * @Version: v1.0
 * 练习1：质数与随机数
 */
public class CollTest3 {
    public static void main(String[] args) {
        //（1）创建一个Collection集合primeNumbers（暂时new ArrayList())
        Collection primeNumbers = new ArrayList();
        //（2）添加100以内的质数到primeNumbers集合中
        for (int i = 2; i < 100; i++) {
            boolean flag = true;
            for (int j = 2; j < i; j++) {
                // 寻找不是质数的数
                if(i % j == 0){
                    flag = false;
                    break;
                }
            }
            if(flag){
                primeNumbers.add(i);
            }
        }
        System.out.println(primeNumbers);
        //质数是大于1的自然数，并且只能被1和它本身整除。

        //（3）查看100以内的质数个数有几个
        int count = primeNumbers.size();
        System.out.println("质数的个数有 : " + count);
        //（4）使用foreach遍历primeNumbers集合中的所有质数。
        for (Object ele : primeNumbers) {
            System.out.println(ele);
        }
        //（5）使用Iterator迭代器删除个位数是3的质数。
        Iterator iterator = primeNumbers.iterator();
        while(iterator.hasNext()){
            Object next = iterator.next();
            Integer num = (Integer)next;
            if(num %10 ==3){
                iterator.remove();
            }
        }
        System.out.println("============================================");
        System.out.println("删除后的集合为" + primeNumbers);
        //（6）判断primeNumbers集合中是否有11，如果有请使用Collection集合的remove方法删除11
        primeNumbers.remove(11);
        //（7）使用Collection集合的removeIf方法删除个位数是7的质数。
        primeNumbers.removeIf(new Predicate() {
            @Override
            public boolean test(Object o) {
                return (Integer)o % 10 == 7;
            }
        });
        //（8）再次使用Iterator遍历primeNumbers集合中剩下的质数。
        Iterator iterator2 = primeNumbers.iterator();
        while (iterator2.hasNext()){
            Object ele = iterator2.next();
            System.out.println(ele);
        }
        //（9）创建另一个Collection集合randNumbers
        Collection randNumbers = new ArrayList();
        //（10）添加10个100以内的随机整数到randNumbers集合中
        Random randomNum = new Random();

        for (int i = 0;i<10;i++){
            int randNum = randomNum.nextInt(100);
            randNumbers.add(randNum);
        }
        //（11）使用foreach遍历randNumbers集合中的随机数。
        for (Object randNumber : randNumbers) {
            System.out.println(randNumber);
        }
        //（12）求它们的交集
        primeNumbers.retainAll(randNumbers);
        System.out.println("交集为" + primeNumbers);
    }
}
