package ML;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vectors;

import java.util.LinkedList;
import java.util.List;

public class base {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("base");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //String[] jarPath = new String[]{"/home/linjiaqin/IdeaProjects/JavaSparkTest/out/artifacts/wordcount/wordcount.jar"};
        //conf.setJars(jarPath);

        String hdfs = "hdfs://localhost:9000";

        LabeledPoint pos = new LabeledPoint(1.0, Vectors.dense(2.0, 3.0, 3.0));
        LabeledPoint neg = new LabeledPoint(0.0, Vectors.sparse(3, new int[] {2, 1,1}, new double[] {1.0, 1.0,1.0}));
        List l = new LinkedList();
        l.add(neg);
        l.add(pos);

        JavaRDD<LabeledPoint> trainData = sc.parallelize(l);


    }
}