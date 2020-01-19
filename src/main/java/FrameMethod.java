import ML.SMSWord;
import RDD.Person;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.ScalaReflection;
import org.apache.spark.sql.hive.HiveContext;
import org.codehaus.janino.Java;

import javax.xml.crypto.Data;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class FrameMethod {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("FrameMethod");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext sqc = new HiveContext(sc);

        String hdfs = "hdfs://localhost:9000";
        String txtFile = hdfs+"/linjiaqin/grade";

        JavaRDD<String>rdd = sc.textFile(txtFile);
        JavaRDD<Person> personRdd = rdd.map(x->{
            String a[] = x.split(" ");
            return new Person(a[0],Integer.parseInt(a[1].trim()));
        });
        //personRdd.foreach(x-> System.out.println(x.name+":"+x.grade));
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        //Dataset<Person> dfPerson = sqc.createDataset(personRdd.rdd(),personEncoder);
        //Dataset<Row> dfPerson = sqc.createDataFrame(personRdd,Person.class);
        List<Person> list = new ArrayList<Person>();
        list.add(new Person("ljq",100));
        list.add(new Person("haha",99));
        list.add(new Person("hehe",8));
        Dataset<Row> dfPerson = sqc.createDataFrame(list,Person.class);
        //Dataset<Row> dfPerson = sqc.createDataFrame(personRdd,Person.class);
        //首先新建一个student的Bean对象，实现序列化和toString()方法，getter,setter方法才行，idea快捷键alt+insert
        //
        //@SuppressWarnings("serial")
        dfPerson.show();
        dfPerson.printSchema();

        ///////////////////////////////////////////////////////////////////////////////////
        String input = hdfs + "/linjiaqin/testjson.json";
        //用sqlContext去初始化，用到哪个Context就用哪个Context去initial
        //Dataset，也是SchemaRDD,他们都是由Row对象组成的RDD
        Dataset<Row> df = sqc.read().json(input);
        df.show();
        df.registerTempTable("people");
        Dataset<Row> sqlDF = sqc.sql("SELECT * FROM people where age > 20");
        sqlDF.show();
        //////////////////////////////////////////////////////////////////////////////////////
        //DataFrame的查询有两种方法，一种是将DataFrame注册成临时表，通过sql语句进行查询
        //第二种是直接在DataFrame上进行查询，是一个lazy操作
        //Dataset<Row> res = df.select("name");
        //res.show();

        //////////////////////////////////////////////////////////////////////////
        //这两种方法也可行，因为Schema和DataFrame都相当于Dataset<Row>,要求
        //Dataset<Row> person = sqc.applySchema(personRdd, Person.class);
        //Dataset<Row> person = sqc.createDataFrame(personRdd, Person.class);
        //////////////////////////////////////////////////////////////////////
        Dataset<Person> person = sqc.createDataset(personRdd.rdd(),personEncoder);
        person.show();
        //从hive表读取
        SparkSession ss = SparkSession.builder().appName("spark sql exmaple")
                .master("local[4]")
                //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                //.enableHiveSupport()
                .getOrCreate();

        Dataset<Row> person4 =ss.table("ljq_test.stu");
        person4.show();

        //String hdfs = "hdfs://localhost:9000";
        String input2 = hdfs+"/linjiaqin/sparkml/SMS";

        //dataset可以用javaRDD()
        JavaRDD<String> dataRdd = sqc.read().textFile(input2).javaRDD();
        JavaRDD<SMSWord> splitRdd = dataRdd.map(s-> {
            String[] two = s.split("\t");
            String[] words = two[1].split(" ");
            return new SMSWord(two[0], words);
        });
        Dataset<Row> sms = sqc.createDataFrame(splitRdd,SMSWord.class);
        //首先新建一个student的Bean对象，实现序列化和toString()方法，getter,setter方法才行，idea快捷键alt+insert
        //
        //@SuppressWarnings("serial")
        sms.show();
        sms.printSchema();

    }
}