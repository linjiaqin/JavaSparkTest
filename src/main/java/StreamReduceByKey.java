import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamReduceByKey {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("StreamReduceByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //String[] jarPath = new String[]{"/home/linjiaqin/IdeaProjects/JavaSparkTest/out/artifacts/wordcount/wordcount.jar"};
        //conf.setJars(jarPath);
        sc.setLogLevel("WARN");
        String hdfs = "hdfs://localhost:9000";
        JavaStreamingContext jsc = new JavaStreamingContext(sc, Duration.apply(1000)); //每3s处理一次

        JavaDStream<String> stream = jsc.socketTextStream("localhost",9999);
        //通过这个等价于JavaPairDStream<String, Integer> pairDStream = stream.flatMaptoPair
        JavaDStream<ReceiveCode> pairDStream = stream.map(x->{
            //String[] e = x.split(" ");
            //return new ReceiveCode(e[0],Integer.valueOf(e[1]));
            // 现将每个次数都设为1
            return new ReceiveCode(x,1);
        });
        JavaDStream<ReceiveCode> windowStream = pairDStream.reduceByWindow((recva,recvb)-> {
            if (recva.buf.equals(recvb.buf)) return new ReceiveCode(recva.buf,recva.num+recvb.num);
            else return recva; //这样写应该没问题，因为它是一行一行的移动下来的，每次比较当前行和下一行
        },Duration.apply(3000), Duration.apply(1000));
        windowStream.count().print();

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