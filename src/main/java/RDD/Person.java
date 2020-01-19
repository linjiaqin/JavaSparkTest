package RDD;
import scala.Serializable;

@SuppressWarnings("serial")
public class Person implements Serializable {
    String name;
    int grade;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getGrade() {
        return grade;
    }

    public void setGrade(int grade) {
        this.grade = grade;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", grade=" + grade +
                '}';
    }

    public Person(String name, int grade) {
        this.name = name;
        this.grade = grade;
    }
}