import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;

public class SocketStream {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[6]").setAppName("SocketStream");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //String[] jarPath = new String[]{"/home/linjiaqin/IdeaProjects/JavaSparkTest/out/artifacts/wordcount/wordcount.jar"};
        //conf.setJars(jarPath);

        String hdfs = "hdfs://localhost:9000";
        sc.setLogLevel("WARN");

        JavaStreamingContext jsc = new JavaStreamingContext(sc, Duration.apply(3000)); //批处理的时间大小
        JavaDStream<String> line = jsc.socketTextStream("localhost",9999);
        //这个采用的是tcp连接，作为客户端，自动处理连接后返回的套接字
        JavaDStream<String> number = line.flatMap(x-> Arrays.asList(x.split(" ")).iterator());
        number.print();
        jsc.start();

        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
            jsc.stop();
        }
        //采用python脚本写的服务器端，每秒钟发送一次
//        解决方法：
//        将 ssc.awaitTerminationOrTimeout(1000)
//        改成ssc.awaitTermination() 即可
    }
}
