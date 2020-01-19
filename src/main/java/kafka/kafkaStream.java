package kafka;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class kafkaStream {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("kafka stream");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));



    }
}
