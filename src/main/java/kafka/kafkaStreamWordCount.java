package kafka;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple1;
import scala.Tuple2;
import scala.tools.scalap.scalax.util.StringUtil;

import java.util.*;

public class kafkaStreamWordCount {
    public static Map<String, Object> initKafka() {
        String brokers = "localhost:9093,localhost:9094";
        String groupId = "testGroup";
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        return kafkaParams;
    }

    public static void windowfunction2(JavaPairRDD<String, Long> rdd){
        List<Tuple2<String,Long>> wordlist = rdd.sortByKey(false).take(10);
        windowfunction1(wordlist);
    }
    public static void windowfunction1(List<Tuple2<String,Long>> wordlist){
        //System.out.println("?？?？?？");
        if (CollectionUtils.isNotEmpty(wordlist)) {
            List<Tuple2<String,Long>> sortList = new ArrayList<Tuple2<String,Long>>(wordlist);
            sortList.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                    if (o2._2.compareTo(o1._2) > 0) return 1;
                    else  if (o2._2.compareTo(o1._2) < 0) return -1;
                    else return 0;
                }
            });
            System.out.println("==================================");
            System.out.println(DateFormatUtils.format(new Date(System.currentTimeMillis()),"HH:mm:ss")+"的热搜词如下");
            for(Tuple2<String, Long> e: sortList){
                System.out.println(e._1+":"+e._2);
            }
            System.out.println("==================================");
        }
    }
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordcountkafka");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(5));
        String hdfs = "hdfs://localhost:9000";
        jssc.checkpoint(hdfs+"/home/linjiaqin/sparkstream");

        Map<String, Object> kafkaParams = initKafka();
        Collection<String> topics = Arrays.asList("topicA","topicB");
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String,String>Subscribe(topics, kafkaParams)
                );
        //JavaDStream<ConsumerRecord<String, String>> keywords1 = stream.cache();
        JavaPairDStream<String, String> keywords = stream.mapToPair(record->{
            return new Tuple2<>(StringUtils.trimToEmpty(record.value()),StringUtils.trimToEmpty(record.value()));
        });
        //keywords.print();

        JavaPairDStream<String,Long> windowstream = keywords.map(value -> value._2())
                .filter((word)->{
                    if ((StringUtils.isBlank(word))) return false;
                    //System.out.println("thisis:"+word);
                    return true;
                })
                .countByValueAndWindow(new Duration(1*20*1000), new Duration(1*20*1000));
        //这里之所以要cache,试音foreach可能会重复使用rdd，这里是个action会导致重复从kafka读取数据
        //windowstream.foreachRDD(recoreds->windowfunction2(recoreds));
        windowstream.cache().foreachRDD(records->windowfunction1(records.sortByKey(false).take(3)));

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
            jssc.close();
        }

    }
}
