import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;
import scala.Array;
import scala.Serializable;
import scala.Tuple1;
import scala.Tuple2;

import java.util.Arrays;

class AvgCount implements Serializable {
    public int num;
    public int total;

    public AvgCount(int num, int total) {
        this.num = num;
        this.total = total;
    }
    public double getAvg(){
        return 1.0*total/num;
    }
}
public class RddAggregate {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("basic transformation").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //distinct usage
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("ljq","ljq","sha","bi"));
        JavaRDD<String> res1 = rdd1.distinct();
        System.out.println(res1.getNumPartitions());
        System.out.println(res1.collect());

        //cartesian usage
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1,2,3,4));
        JavaPairRDD<Integer, String> res2 = rdd2.cartesian(rdd1);
        System.out.println(res2.collect());

        //aggregate usage
        /*
        这里采用元组去表示数据，其实还可以不用元组的表示方法，还可以用类，因为本质上就是数据的赋值访问，只是外面套不一样的格式
        访问的方式不一样而已
         */
        Tuple2 avgtuple = rdd2.aggregate(new Tuple2<>(0,0),(a,x)->new Tuple2<>(a._1+x,a._2+1),(a,b)->new Tuple2<>(a._1+b._1,a._2+b._2));
        double avg = Double.valueOf(avgtuple._1.toString())/Double.valueOf(avgtuple._2.toString());
        System.out.println("average is:"+avg);
        /*
        *类的方法
        *def aggregate[U](zeroValue : U)(seqOp : Function2[U, T, U], combOp : Function2[U, U, U]) : U = { compiled code }
        * 可以看到这里函数的定义只要U类型确定，后面的function的类型也一一对应的确定了
        */
        AvgCount avgCount = rdd2.aggregate(new AvgCount(0,0),(a,x)->new AvgCount(a.num+1,a.total+x),
                (a,b)-> new AvgCount(a.num+b.num, a.total+b.total));
        System.out.println("average is:"+avgCount.getAvg());

    }
}
