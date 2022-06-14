package bean;

/**
 * @ClassName: Student
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月14日 21时43分
 * @Version: v1.0
 *  声明学员类型Student，包含属性：学号，姓名，年龄，属性私有化，提供有参构造，get/set，重写toString
 */
public class Student {
    private Integer id;
    private String name;
    private Integer age;

    public Student() {
    }

    public Student(Integer id, String name, Integer age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
