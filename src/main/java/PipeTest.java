import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;

import java.util.Arrays;

public class PipeTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("PipeTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //String[] jarPath = new String[]{"/home/linjiaqin/IdeaProjects/JavaSparkTest/out/artifacts/wordcount/wordcount.jar"};
        //conf.setJars(jarPath);

        String hdfs = "hdfs://localhost:9000";
        String cPath = "file:/home/linjiaqin/sh/hello";

        sc.addFile(cPath);
        JavaRDD<Integer> a = sc.parallelize(Arrays.asList(1,2,3,4,5,6));
        JavaRDD<String> rdd = a.pipe(SparkFiles.get("hello"));
        rdd.collect().forEach(x-> System.out.println(x));

    }
}