package ML;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

public class LibSVM {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("libsvm").setMaster("local[*]");
        SparkContext sc = new SparkContext(conf);

    }
}
