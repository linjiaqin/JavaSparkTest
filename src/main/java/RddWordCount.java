import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class RddWordCount {
    public static void main(String[] agrs) {
        SparkConf conf = new SparkConf().setAppName("wordcount");//spark://127.0.0.1:7077
        JavaSparkContext sc = new JavaSparkContext(conf);

        //String[] jarPath = new String[]{"/home/linjiaqin/IdeaProjects/JavaSparkTest/out/artifacts/wordcount/wordcount.jar"};
        //conf.setJars(jarPath);

        String hdfs = "hdfs://localhost:9000";
        String input = hdfs + "/linjiaqin/a.txt";

        JavaRDD<String> text = sc.textFile(input);

        JavaRDD<String> words = text.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> counts = words.mapToPair(x -> new Tuple2<>(x, 1)).reduceByKey((a,b) -> a+b);
        //List<String> wordsList = words.collect();
        //wordsList.forEach(x -> System.out.println(x));
        JavaPairRDD<String, Integer> sortCounts = counts.sortByKey();
        List countlist = sortCounts.collect();
        countlist.forEach(x -> System.out.println(x));

        //JavaRDD<String> words =  text.filter(line -> line.contains("python"));
//        JavaRDD<String> flatMapRDD = text.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String s) throws Exception {
//                String[] split = s.split("\\s+");
//                return Arrays.asList(split).iterator();
//            }
//        });
        //        for(String x: wordsList)
        //        System.out.println(x);



    }
}
