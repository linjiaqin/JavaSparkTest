package ML;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public class Logistic {
    public static void main(String[] args) {
        // 1. spark配置
        SparkConf conf = new SparkConf()
                .setAppName("logistic")
                .setMaster("local[*]");
        //2.sparksession的配置
        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
        //3. 数据读取
        //JavaRDD<>
    }
}
