import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.datanucleus.store.types.backed.ArrayList;

import java.util.Arrays;
import java.util.List;

class Count implements Function<Integer, Integer> {
    int count;
    public Count(){
        count = 0;
    }
    @Override
    public Integer call(Integer o) throws Exception {
        count = count+1;
        System.out.println(o+":count:"+count);
        return o+1;  //这里不能自加
    }
}
public class RddAccmulator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("RddAccmulator");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Count count = new Count();  //这里属于驱动器的变量，在每个task都会有一个
        JavaRDD<Integer> list = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10)).repartition(2);
        list.map(count).collect().forEach(x-> System.out.println(x));
        System.out.println("drivercount:"+count.count);

    }
}