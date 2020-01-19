package ML;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import scala.collection.immutable.List;

import javax.xml.crypto.Data;

public class MLP {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("ML.LinearRegression");

        //String[] jarPath = new String[]{"/home/linjiaqin/IdeaProjects/JavaSparkTest/out/artifacts/wordcount/wordcount.jar"};
        //conf.setJars(jarPath);

        SparkSession spark = SparkSession.
                builder().
                config(conf).
                getOrCreate();

        String hdfs = "hdfs://localhost:9000";
        String input = hdfs + "/linjiaqin/sparkml/SMS";

        //从sparkSession获取sparkcontext
        //JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        //但是sparkSession也可以直接读取
        //dataset可以用javaRDD()
        JavaRDD<String> dataRdd = spark.read().textFile(input).javaRDD();
        JavaRDD<SMSWord> splitRdd = dataRdd.map(s -> {
            String[] two = s.split("\t");
            String[] words = two[1].split(" ");
            return new SMSWord(two[0], words);
        });
        System.out.println(splitRdd.count());
        //splitRdd.collect().forEach(x -> System.out.println(x));
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

        //​ StringIndexer是指把一组字符型标签编码成一组标签索引，索引的范围为0到标签数量，
        // 索引构建的顺序为标签的频率，优先编码频率较大的标签，所以出现频率最高的标签为0号
        StringIndexer stringIndexer = new StringIndexer()
                .setInputCol("label")
                .setOutputCol("indexlabel");
                //.fit(smsDF);
        Dataset<Row> indexDF = stringIndexer.fit(smsDF).transform(smsDF);
        indexDF.show();
        indexDF.printSchema();


        Word2Vec word2Vec = new Word2Vec()
                .setInputCol("words")
                .setOutputCol("features")
                .setVectorSize(100)
                .setMaxSentenceLength(1);
        Dataset<Row> word2vec = word2Vec.fit(smsDF).transform(smsDF);
        word2vec.show();

        //创建多层感知机
        int[] layers = new int[]{100, 6, 5, 3};
        MultilayerPerceptronClassifier mlp = new MultilayerPerceptronClassifier()
                .setLayers(layers)
                .setBlockSize(512)
                .setSeed(1234L)
                .setMaxIter(128)
                .setLabelCol("indexlabel")
                .setFeaturesCol("features")
                .setPredictionCol("predictindexlabel");

        //这里的IndexToString比较奇怪，是一个transformer不是estimator
        IndexToString indexToString = new IndexToString()
                .setInputCol("predictindexlabel")
                .setOutputCol("predictlabel")
                .setLabels(new String[]{"ham","spam"});
//        Dataset<Row> indextostring = indexToString.transform(indexDF);
//        indextostring.show();

        Dataset<Row>[] data= smsDF.randomSplit(new double[]{0.8,0.2});
        Dataset<Row> trainData = data[0];
        Dataset<Row> testData = data[1];

        Pipeline pipeline = new Pipeline();
        pipeline.setStages(new PipelineStage[]{stringIndexer,word2Vec,mlp,indexToString});
        PipelineModel model = pipeline.fit(trainData);
        Dataset<Row>predicts = model.transform(testData);
        predicts.show();
        predicts.printSchema();
        for (Row r: predicts.select("label","indexlabel","predictindexlabel","predictlabel").collectAsList()) {
            System.out.println(r);
        }
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();
        evaluator.setLabelCol("indexlabel").setPredictionCol("predictindexlabel");
        double acc = evaluator.evaluate(predicts);
        System.out.println(String.format("Accuracy is %2.4f",acc));

        final LongAccumulator accumulator = spark.sparkContext().longAccumulator();
        JavaPairRDD<Long, Row> predictRdd = predicts.select("label","predictlabel").javaRDD().zipWithUniqueId().mapToPair(x->{
            String a = x._1.get(0).toString();
            String b = x._1.get(1).toString();
            if (!a.equals(b)) accumulator.add(1);
            return new Tuple2<Long, Row>(x._2, x._1);
        });
        predictRdd.collect().forEach(x-> System.out.println(x._1+","+x._2));
        System.out.println("accumulator is:" + accumulator.value());
    }
}
