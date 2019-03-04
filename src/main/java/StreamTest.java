import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/*
这里的粒度是rdd的粒度,DStream的粒度是rdd
 */
class MyFilter implements Function<JavaRDD<String>,JavaRDD<String>> {

    @Override
    public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
        //下面这几种操作都行
        //return rdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).filter(x->x.contains("I"));
        //return rdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).map(x->x+" hello");
        return rdd.flatMap(new MyFilters());
    }
}

class MyFilters implements FlatMapFunction<String,String> {
    @Override
    public Iterator<String> call(String s) throws Exception {
        String[] split = s.split("\\s+");
        return Arrays.asList(split).iterator();
    }
}

//这样不行，因为从rdd变成了pairrdd，但是transform是从rdd到rdd的转换
class MyFlat implements Function<JavaRDD<String>, JavaPairRDD<String,Integer>> {

    @Override
    public JavaPairRDD<String,Integer> call(JavaRDD<String> rdd) throws Exception {
        //return rdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).filter(x->x.contains("I"));
        return rdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).mapToPair(x->new Tuple2<>(x,1));
    }
}

public class StreamTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("StreamTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //String[] jarPath = new String[]{"/home/linjiaqin/IdeaProjects/JavaSparkTest/out/artifacts/wordcount/wordcount.jar"};
        //conf.setJars(jarPath);

        String hdfs = "hdfs://localhost:9000";

        sc.setLogLevel("WARN");
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Duration.apply(4000));
        JavaDStream<String> lines = ssc.socketTextStream("localhost",9999);

        JavaDStream<String> words = lines.transform(new MyFilter());
        JavaPairDStream<String,Integer> wordCount = words.mapToPair(x->new Tuple2<>(x,1)).reduceByKey((a,b)->a+b);

        wordCount.print();
        ssc.start();
        try {
            ssc.awaitTerminationOrTimeout(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
}