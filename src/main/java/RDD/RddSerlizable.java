package RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Serializable;
import java.util.ArrayList;
import java.util.List;

public class RddSerlizable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("RddSerlizable");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //String[] jarPath = new String[]{"/home/linjiaqin/IdeaProjects/JavaSparkTest/out/artifacts/wordcount/wordcount.jar"};
        //conf.setJars(jarPath);

        String hdfs = "hdfs://localhost:9000";
        String output = hdfs+"/linjiaqin/PersonObject";
        List<Person> list = new ArrayList<Person>();
        list.add(new Person("ljq",100));
        list.add(new Person("haha",99));
        list.add(new Person("hehe",8));
        JavaRDD<Person> rdd = sc.parallelize(list);
        //rdd.collect().forEach(x -> System.out.println(x.name+";"+x.grade));
        JavaRDD<Person> rdd1 = rdd.filter(x->x.grade > 60);
        System.out.println(rdd1.collect());
        //rdd.saveAsObjectFile(output);

    }
}