package RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class RddParallelize {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("squar").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5));
        JavaRDD<Integer> res = rdd.map(x -> x*x);
        System.out.println(res.collect());
    }
}
