package ML

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object LinearRegressionScale {
  case class SMSWordz(label:String, words:Array[String])
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext;
    val hdfs = "hdfs://localhost:9000";
    val input = hdfs+"/linjiaqin/sparkml/SMS";
    val datardd = sc.textFile(input).map(_.split("\t"))
      .map(line=>SMSWordz(line(0), line(1).split(" ")))
    val df = spark.createDataFrame(datardd).toDF("label","words")
    df.show();
  }

}
