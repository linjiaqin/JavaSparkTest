package Stream.NetworkWordCounts;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * 这个是自定义数据源
 * JavaReceiverInputDStream就是用于自定义数据源
 * 如果实现的输入流需要在工作结点运行一个接收器，使用JavaReceiverInputDStream
 */
public class ReceiverStream extends Receiver<String> {
    String host = null;
    int port = -1;

    public ReceiverStream(StorageLevel storageLevel, String host, int port) {
        super(storageLevel);
        this.host = host;
        this.port = port;
    }

    private static final Pattern SPACE = Pattern.compile(" ");
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaCustomReceiver");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));

        JavaReceiverInputDStream<String> lines = ssc.socketStream(
                new JavaCus
        )
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        wordCounts.print();
        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onStart() {

    }

    @Override
    public void onStop() {

    }

    private void receive()
}
