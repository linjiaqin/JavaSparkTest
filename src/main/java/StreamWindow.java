import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;

class ReceiveCode implements Serializable{
    String buf;
    int num;

    public ReceiveCode(String buf, int num) {
        this.buf = buf;
        this.num = num;
    }
}
public class StreamWindow {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("StreamWindow");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //String[] jarPath = new String[]{"/home/linjiaqin/IdeaProjects/JavaSparkTest/out/artifacts/wordcount/wordcount.jar"};
        //conf.setJars(jarPath);
        sc.setLogLevel("WARN");
        String hdfs = "hdfs://localhost:9000";
        JavaStreamingContext jsc = new JavaStreamingContext(sc, Duration.apply(1000)); //每3s处理一次

        JavaDStream<String> stream = jsc.socketTextStream("localhost",9999);
        //通过这个等价于JavaPairDStream<String, Integer> pairDStream = stream.flatMaptoPair
        JavaDStream<ReceiveCode> pairDStream = stream.map(x->{
           String[] e = x.split(" ");
           return new ReceiveCode(e[0],Integer.valueOf(e[1]));
        });
        JavaDStream<ReceiveCode> windowStream = pairDStream.window(Duration.apply(3000), Duration.apply(1000));
        windowStream.foreachRDD(x-> {
            x.collect().forEach(recv-> System.out.println(recv.buf+","+recv.num));
        });

        jsc.start();

        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            jsc.stop();
            e.printStackTrace();
        }


    }
}