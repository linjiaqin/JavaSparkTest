import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;


public class RddSerializable1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("RddSerlizable");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //String[] jarPath = new String[]{"/home/linjiaqin/IdeaProjects/JavaSparkTest/out/artifacts/wordcount/wordcount.jar"};
        //conf.setJars(jarPath);

        String hdfs = "hdfs://localhost:9000";
        String input = hdfs+"/linjiaqin/grade";
        String output = hdfs+"/linjiaqin/PersonObject1";

        JavaRDD<String> rdd = sc.textFile(input);
        //第一种方法：显式转化
        JavaRDD<Person> personRdd = rdd.map(x->{
            String a[] = x.split(" ");
            return new Person(a[0],Integer.valueOf(a[1]));
        });
        personRdd.collect().forEach(x -> System.out.println(x.name+";"+x.grade));
        System.out.println(personRdd.collect());

        //第二种方法：其实两种都是一样的
        JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(x->{
            String a[] = x.split(" ");
            return new Tuple2<>(a[0],Integer.valueOf(a[1]));
        });
        JavaRDD<Person> personJavaRDD = pairRDD.map(x->new Person(x._1,x._2));
        //这个目前没有直接的方法，只有先转成map的形式后面再转回来
//        JavaRDD<Person> reduceJavaRDD = personJavaRDD.map((person1,person2)->{
//            if (person1.name.equals(person1.name)) return new Person(person1.name,person1.grade+person2.grade);
//            else return person1;
//        });
        //JavaRDD<Person> persons = personJavaRDD.groupByKey()错误，这里只能针对tuple的rdd，自己定义的类型不行
        personJavaRDD.collect().forEach(x -> System.out.println(x.name+";"+x.grade));
        System.out.println(personJavaRDD.collect());
    }
}