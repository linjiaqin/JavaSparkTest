import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import javax.jnlp.IntegrationService;
import java.util.Arrays;
import java.util.List;

public class RddBroadcast {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("RddBroadcast");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //String[] jarPath = new String[]{"/home/linjiaqin/IdeaProjects/JavaSparkTest/out/artifacts/wordcount/wordcount.jar"};
        //conf.setJars(jarPath);

        String hdfs = "hdfs://localhost:9000";
        JavaRDD<Integer> list = sc.parallelize(Arrays.asList(1,2,3,4,5,6,1,2,3,7,8,9,10)).repartition(3);
        final Broadcast<List> bC = sc.broadcast(Arrays.asList(1,2,3));
        JavaRDD<Integer> rdd = list.filter(x->{
            return !bC.value().contains(x);
        });
        System.out.println(rdd.collect());

    }
}