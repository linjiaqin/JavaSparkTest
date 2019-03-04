import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;


class MyFilter implements Function<JavaRDD<String>,JavaRDD<String>> {

    @Override
    public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
        return rdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).filter(x->x.contains("I"));
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

        words.print();
        ssc.start();
        try {
            ssc.awaitTerminationOrTimeout(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
}