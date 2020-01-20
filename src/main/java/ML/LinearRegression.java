package ML;
import ML.typical.SMSWord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.sql.*;

public class LinearRegression {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("ML.LinearRegression");

        //String[] jarPath = new String[]{"/home/linjiaqin/IdeaProjects/JavaSparkTest/out/artifacts/wordcount/wordcount.jar"};
        //conf.setJars(jarPath);

        SparkSession spark = SparkSession.
                builder().
                config(conf).
                getOrCreate();

        String hdfs = "hdfs://localhost:9000";
        String input = hdfs+"/linjiaqin/sparkml/SMS";

        //从sparkSession获取sparkcontext
        //JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        //但是sparkSession也可以直接读取
        //dataset可以用javaRDD()
        JavaRDD<String> dataRdd = spark.read().textFile(input).javaRDD();
        JavaRDD<SMSWord> splitRdd = dataRdd.map(s-> {
                String[] two = s.split("\t");
                String[] words = two[1].split(" ");
                return new SMSWord(two[0], words);
        });

        splitRdd.collect().forEach(x-> System.out.println(x));
        Dataset<Row> smsDF = spark.createDataFrame(splitRdd, SMSWord.class);
                //.toDF("labelcol","wordsCol");
        smsDF.printSchema();
        smsDF.show();
//        smsDF.registerTempTable("SMS");
//        Dataset<Row> sqlDF = spark.sql("SELECT * FROM SMS");
//        sqlDF.show();

//        Encoder<SMSWord> personEncoder = Encoders.bean(SMSWord.class);
//        Dataset<SMSWord> smsDF = spark.createDataset(splitRdd.rdd(), personEncoder);
//        smsDF.printSchema();
//        smsDF.show();

         StringIndexerModel stringIndexer = new StringIndexer()
                .setInputCol("label")
                .setOutputCol("newlabel")
                .fit(smsDF);

        Word2Vec word2Vec = new Word2Vec()
                .setInputCol("word")
                .setOutputCol("features")
                .setVectorSize(100)
                .setMaxSentenceLength(1);
        int[] layers = new int[]{100, 6, 5, 3};


    }
}