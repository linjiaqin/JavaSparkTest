import org.apache.spark.{SparkConf, SparkContext}
import scala.sys.process._
object SysProcessTest {
  def main(args: Array[String])={
    val conf = new SparkConf().setMaster("local[4]").setAppName("sysTest");
    val sc = new SparkContext(conf);

    //val cPath = "/home/linjiaqin/sh/hello" !   //这里就不是pipe的那种语法了，这里的路径是系统的实际路径
    val rdd = sc.parallelize(List(1,2,3,4,5,6))
    //val output = rdd.foreach(x => "/home/linjiaqin/sh/hello" !) !放回int， !!String
    //val output = rdd.map(x => "/home/linjiaqin/sh/hello".!!)
    val output = rdd.map(x=> Process("/home/linjiaqin/sh/hello2 "+x).!!)
    output.collect().foreach(x=>println("out"+x))
  }
}
