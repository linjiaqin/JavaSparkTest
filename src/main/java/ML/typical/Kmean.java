package ML.typical;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.SQLDataTypes;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

/**
 * kmeans聚类的原理
 * (1)选择初始k个点作为初始化的聚类中心
 * (2)计算其余所有点到这K个聚类点的距离，划分到距离最近的点中去
 * (3)重新计算每个聚类簇的平均值点，作为聚类中心
 * (4)重复2,3直到聚类中心不再改变或者迭代次数达到预定值
 */
public class Kmean {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("tfidf");

        String hdfs = "hdfs://localhost:9000";
        String input = hdfs + "/linjiaqin/sparkml/Wholesale_customers_data.csv";

        SparkSession spark = SparkSession.
                builder().
                config(conf).
                getOrCreate();

        //JavaRDD<String> rdd = spark.read().textFile()

        Dataset<Row> data = spark.read()
                .option("header",true)
                .option("inferSchema", true) //自动推断，否则默认为String类型
                .csv(input);
        data.show();
        data.printSchema();
        //这里看到输出的每一列都是string，不行，得转成double
        String[] columnName = data.columns();

        //这样子还是不行，因为ml要求的数据格式是vector，虽然这样子满足了features属性名
        StructType schema = new StructType(new StructField[]{
                new StructField("features", new ArrayType(DataTypes.IntegerType, true), false, Metadata.empty())
        });
        Dataset<Row> schemaRdd = spark.createDataFrame(data.rdd(), schema);
        schemaRdd.printSchema();
        //schemaRdd.show();

        //因为字段推断为int，但是vector默认是double，所以会发生类型冲突
        StructType schema1 = new StructType(new StructField[]{
                //new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("features", SQLDataTypes.VectorType(), false, Metadata.empty()),
        });
        Dataset<Row> schema1Rdd = spark.createDataFrame(data.rdd(), schema1);
        schema1Rdd.printSchema();
        //schema1Rdd.show();
        //这里会看到kmeans模型要求的输入属性名默认是features，也就是说它实际是封装mllib的
        //基本没有什么改变，和mllib一样要求输入是vector，所以这里得把原来的转成特征值通用
        //其实所有的这些模型默认输入是label，属性是string，features，属性是Vector。
        //features是由多个属性组成的，只不过最后统一成一个Vector，可以看到MLP中就是如此

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(columnName)
                .setOutputCol("features");
        Dataset<Row> traindata = assembler.transform(data);
        traindata.printSchema();
        traindata.show();

        int numClusters = 3;
        int numiter = 50;
        KMeans kMeans = new KMeans();
        kMeans.setK(numClusters).setMaxIter(numiter).setSeed(1L);
        KMeansModel model = kMeans.fit(traindata);
        Dataset<Row>predictions = model.transform(traindata);
        //model.predict();

        double WSSSE = model.computeCost(traindata);
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

        Vector[] centers = model.clusterCenters();
        for (Vector e: centers) {
            System.out.println(e);
        }

        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{assembler, kMeans});
        PipelineModel pipelineModel = pipeline.fit(data);
        Dataset<Row>prediction2 = pipelineModel.transform(data);
        prediction2.show();
    }
}
