package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class kafkaProducer {
    //创建 Kafka 生产者时，以下三个属性是必须指定的：
    //
    //bootstrap.servers ：指定 broker 的地址清单，清单里不需要包含所有的 broker 地址，生产者会从给定的 broker 里查找 broker 的信息。不过建议至少要提供两个 broker 的信息作为容错；
    //key.serializer ：指定键的序列化器；
    //value.serializer ：指定值的序列化器。

    public static void main(String[] args) {
        String topicName = "topicA";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9093,localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

//        /*传递自定义分区器*/
//        props.put("partitioner.class", "com.heibaiying.producers.partitioners.CustomPartitioner");
//        /*传递分区器所需的参数*/
//        props.put("pass.line", 6);

        Producer<Integer, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i <= 1000; i++) {
            String score = "score:" + i;
            ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName, i, score);
            /*异步发送消息*/
            producer.send(record, (metadata, exception) ->
                    System.out.printf("%s, partition=%d, \n", score, metadata.partition()));
        }

        producer.close();

    }
}
