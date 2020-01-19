package ML;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Row;


import java.util.Arrays;
import java.util.List;

public class TfIdf {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("tfidf");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String hdfs = "hdfs://localhost:9000";
        String input = hdfs+"/linjiaqin/book/ch8/data/tf-idf.txt";
        JavaRDD<String> rdd = sc.textFile(input);
//        JavaRDD<Vector> data = rdd.map(x -> x.split(" "));
//        HashingTF hashingTF = new HashingTF()
        JavaRDD<Vector> traindata = rdd.map(new Function<String, Vector>() {
            @Override
            public Vector call(String s) throws Exception {
                String[] tmp = s.split(" ");
                double[] features = new double[tmp.length];
                int i = 0;
                for(String e:tmp) {
                    features[i++] = Double.parseDouble(e);
                }
                return Vectors.dense(features);
            }
        });

    }
}
