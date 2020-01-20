package ML.typical;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.linalg.SQLDataTypes;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

public class RandomForest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("RandomForest");

        //String[] jarPath = new String[]{"/home/linjiaqin/IdeaProjects/JavaSparkTest/out/artifacts/wordcount/wordcount.jar"};
        //conf.setJars(jarPath);

        String hdfs = "hdfs://localhost:9000";
        String input = hdfs+"/linjiaqin/sparkml/data_banknote_authentication.txt";

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
//        Dataset<Row> data = spark.read().text(input);
//        data.printSchema();
//        data.show();
        JavaRDD<String> data = spark.read().textFile(input).javaRDD();

        JavaRDD<Row> data1 = data.map(x->{
            String[] line = x.split(",");
            double[] fea = new double[line.length-1];
            for (int i = 0; i < line.length-1; i++) {
                fea[i] = Double.parseDouble(line[i]);
            }
            Vector vec = Vectors.dense(fea);
            return RowFactory.create(vec, line[line.length-1]);
            //return RowFactory.create(fea, line[line.length-1]);
        });

        StructType schema = new StructType(new StructField[]{
                new StructField("features", SQLDataTypes.VectorType(), false, Metadata.empty()),
                new StructField("label", DataTypes.StringType, false, Metadata.empty()),
        });
        Dataset<Row> df = spark.createDataFrame(data1, schema);
        df.printSchema();
        df.show();
        Dataset<Row>[] dfs = df.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> train = dfs[0];
        Dataset<Row> test = dfs[1];

        StringIndexer string2index = new StringIndexer()
                .setInputCol("label")
                .setOutputCol("indexlabel");

        IndexToString index2String = new IndexToString()
                .setInputCol("indexlabel")
                .setOutputCol("predictlabel")
                .setLabels(new String[]{"yes","no"});
        RandomForestClassifier classifier = new RandomForestClassifier();
        classifier.setLabelCol("indexlabel")
                .setFeaturesCol("features")
                .setNumTrees(5);
        Pipeline pipeline = new Pipeline();
        pipeline.setStages(new PipelineStage[]{string2index,classifier, index2String});
        PipelineModel model = pipeline.fit(train);
        Dataset<Row> predict = model.transform(test);
        predict.show();
    }
}