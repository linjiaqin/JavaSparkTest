package RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;


import java.util.Arrays;

public class RddPipe {
    public static void main(String[] args) {
        //SparkConf conf = new SparkConf().setMaster("spark://192.168.0.100:7077").setAppName("PipeTest");
        SparkConf conf = new SparkConf().setMaster("local").setAppName("PipeTest");
        JavaSparkContext sc = new JavaSparkContext(conf);


        String cPath = "file:/home/linjiaqin/sh/sparktest/hello1";
        sc.addFile(cPath,true);    //很玄学，这里需要true，然后运行那里不是
        String jarPath= "/home/linjiaqin/IdeaProjects/JavaSparkTest/out/artifacts/RddPipe/RddPipe.jar";
        sc.addJar(jarPath);
        JavaRDD<Integer> a = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,9,10,11,12,13,14,15,16,17,18,19,20)).repartition(4);   //这里rdd作为参数输入
        a.collect().forEach(x-> System.out.println(SparkFiles.get("hello1")));
        //a.foreach(x-> System.out.println(SparkFiles.get("hello")));
        JavaRDD<String> rdd = a.pipe("./hello1");     //SparkFiles.get("hello")local模式的时候用这个就行
        //可以看出开的进程数和核数相关,与rdd的个数无关，而且还和cin的接收有关
        //如果每个c程序只接受一个参数，只会执行核数次而不是将所有的rdd都执行完
        rdd.collect().forEach(x-> System.out.println("haha"+x));
        JavaRDD<Integer> b = a.map(x->x+1);
        b.collect().forEach(x-> System.out.println(x));
    }
}