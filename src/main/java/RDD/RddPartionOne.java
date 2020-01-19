package RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class MyCount{
    public  static int count = 0;
    public static void add(){
        count++;
    }
    public MyCount(){
        count++;
    }
    public static int getcount(){
        return count;
    }
}
public class RddPartionOne {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("RddAccmulator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> list = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10)).repartition(2);

        //list.map(count).collect().forEach(x-> System.out.println(x));
        //System.out.println("drivercount:"+count.count);
        JavaRDD<String> rddRes = list.mapPartitionsWithIndex((x,it)->{
            List<String> sumList = new ArrayList<String>();
            StringBuffer num  = new StringBuffer();
            while(it.hasNext()){
                num = num.append(it.next()+",");
            }
            sumList.add(x+"|"+num);
            return sumList.iterator();
        },false);
        System.out.println(rddRes.count());
        System.out.println(rddRes.collect());

        JavaRDD<String> rdd2 = list.map(x->{
            MyCount a = new MyCount();
            System.out.println(MyCount.getcount());
            return x.toString();
        });
        rdd2.collect();
    }
}