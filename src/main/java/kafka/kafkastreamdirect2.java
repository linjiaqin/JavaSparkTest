package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.LocationStrategy;
import scala.Tuple2;

import java.util.*;

public class kafkastreamdirect2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
//		conf.setMaster("local[4]");
        conf.setMaster("local[*]");
        conf.setAppName("kafkastream");
        conf.set("spark.streaming.stopGracefullyOnShutdown","true");
        conf.set("spark.default.parallelism", "6");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        JavaStreamingContext jssc = new JavaStreamingContext(javaSparkContext, Durations.seconds(10));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9093,localhost:9094");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "testGroup");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", true);

//        Collection<String> topic0 = Arrays.asList("topic0");
//        Collection<String> topic1 = Arrays.asList("topic1");
//        Collection<String> topic2 = Arrays.asList("topic2");
//
//        List<Collection<String>> topics = Arrays.asList(topic0, topic1, topic2);
//        List<JavaDStream<ConsumerRecord<String, String>>> kafkaStreams = new ArrayList<>(topics.size());
//
//        for(int i = 0; i < topics.size(); i++){
//            kafkaStreams.add(
//                    KafkaUtils.createDirectStream(
//                            jssc,
//                            LocationStrategies.PreferConsistent(),
//                            ConsumerStrategies.<String, String>Subscribe(topics.get(i), kafkaParams)
//                    ));
//        }
        Collection<String> topics = Arrays.asList("topicA","topicB");
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String,String>Subscribe(topics, kafkaParams)
                );
        JavaPairDStream<String,String> stream1 = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
        stream1.print();

        // Start the computation
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
