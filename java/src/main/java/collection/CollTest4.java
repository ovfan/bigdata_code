package collection;

import bean.Student;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Predicate;

/**
 * @ClassName: CollTest4
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月14日 21时42分
 * @Version: v1.0
 * 练习2：学生对象
 */
public class CollTest4 {
    public static void main(String[] args) {
        //（1）声明学员类型Student，包含属性：学号，姓名，年龄，属性私有化，提供有参构造，get/set，重写toString

        //（2）创建Collection集合students（暂时new ArrayList())
        Collection students = new ArrayList();
        //（3）添加本组学员Student对象到students集合中
        Student stu1 = new Student(1, "李子阳", 18);
        Student stu2 = new Student(2, "fanfan", 17);
        Student stu3 = new Student(3, "tongtong", 17);
        Student stu4 = new Student(4, "tingting", 18);
        students.add(stu1);
        students.add(stu2);
        students.add(stu3);
        students.add(stu4);
        //（4）使用foreach遍历
        for (Object student : students) {
            System.out.println(student);
        }
        //（5）使用Collection的remove方法删除自己，思考这样是否可以删除，如果不能，怎么办
//        students.remove(stu2);
        // System.out.println(students);
        //```java
        //        students.remove(new Student(自己的学号,"自己的姓名",自己的年龄));
        //```
        //
        //（6）使用Collection的removeIf方法删除和自己年龄一样的组员
        // 获取fanfan的年龄
        Integer fanfanAge = stu2.getAge();
        students.removeIf(new Predicate() {
            @Override
            public boolean test(Object o) {

                Student s = (Student) o;

                return s.getAge().equals(fanfanAge);
            }
        });
        System.out.println(students);
        //（7）使用Iterator遍历，并根据组长姓名删除组长
        Iterator iterator = students.iterator();
        while(iterator.hasNext()){

            if(((Student)iterator.next()).getName() == stu1.getName()){
                iterator.remove();
            }
        }
        System.out.println(students);
        //（8）最后再次使用foreach遍历students集合
        for (Object student : students) {
            System.out.println(student);
        }
    }
}
