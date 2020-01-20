package ML.statistic;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.util.Arrays;
import java.util.List;

public class TfidfExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("tfidf");
        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
        List<Row> data = Arrays.asList(
                RowFactory.create(0.0, "Hi I heard about Spark"),
                RowFactory.create(0.0, "I wish Java could use case classes"),
                RowFactory.create(1.0, "Logistic regression models are neat")
        );

        StructType schema = new StructType( new StructField[]{
           new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
           new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
        });
    }
}
