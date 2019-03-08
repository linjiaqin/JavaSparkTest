import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;

import java.util.Arrays;

public class RddPipe {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("PipeTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //String[] jarPath = new String[]{"/home/linjiaqin/IdeaProjects/JavaSparkTest/out/artifacts/wordcount/wordcount.jar"};
        //conf.setJars(jarPath);

        String hdfs = "hdfs://localhost:9000";
        String cPath = "file:/home/linjiaqin/sh/hello";

        sc.addFile(cPath);
        JavaRDD<Integer> a = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,9));   //这里rdd作为参数输入
        JavaRDD<String> rdd = a.pipe(SparkFiles.get("hello"));       //可以看出开的进程数和核数相关,与rdd的个数无关，而且还和cin的接收有关
        //如果每个c程序只接受一个参数，只会执行核数次而不是将所有的rdd都执行完
        rdd.collect().forEach(x-> System.out.println("haha"+x));
        JavaRDD<Integer> b = a.map(x->x+1);
        b.collect().forEach(x-> System.out.println(x));
    }
}