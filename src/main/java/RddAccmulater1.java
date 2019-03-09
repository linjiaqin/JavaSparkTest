import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import java.util.Arrays;

public class RddAccmulater1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("RddAccmulator");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        final Accumulator<Integer> count1 = sc.accumulator(0);
        final Accumulator<Integer> count2 = sc.accumulator(0);
        JavaRDD<Integer> list = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10)).repartition(2);
        list.foreach(x->{count1.add(1);});
        System.out.println(list.collect());
        System.out.println(count1.value());  //这个得在action操作之后，否则lazy求值未action之前transf操作都不执行

        JavaRDD<Integer> rdd = list.map(x->{
            count2.add(1);
            return x+1;
        });
        System.out.println(rdd.count());
        System.out.println(count2.value());
        System.out.println(rdd.reduce((x,y)->x+y));
        System.out.println(count2.value());
        System.out.println(rdd.collect());
    }
}
