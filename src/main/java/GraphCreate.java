import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.graphx.*;
import org.apache.spark.graphx.VertexRDD;

import org.apache.spark.graphx.GraphLoader;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

//@SuppressWarnings("serial")
//class Edge{
//    long srcId;
//    long dstId;
//    String attr;
//
//    public Edge(long srcId, long dstId, String attr) {
//        this.srcId = srcId;
//        this.dstId = dstId;
//        this.attr = attr;
//    }
//
//    public String getAttr() {
//        return attr;
//    }
//
//    public long getSrcId() {
//        return srcId;
//    }
//
//    public long getDstId() {
//        return dstId;
//    }
//
//    public void setSrcId(long srcId) {
//        this.srcId = srcId;
//    }
//
//    public void setDstId(long dstId) {
//        this.dstId = dstId;
//    }
//
//    public void setAttr(String attr) {
//        this.attr = attr;
//    }
//
//    @Override
//    public String toString() {
//        return "Edge{" +
//                "srcId=" + srcId +
//                ", dstId=" + dstId +
//                ", attr='" + attr + '\'' +
//                '}';
//    }
//}
public class GraphCreate {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("GraphCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //String[] jarPath = new String[]{"/home/linjiaqin/IdeaProjects/JavaSparkTest/out/artifacts/wordcount/wordcount.jar"};
        //conf.setJars(jarPath);

        String hdfs = "hdfs://localhost:9000";
        String input = hdfs + "/linjiaqin/GraphTest/edges.txt";

//         JavaRDD<Tuple2<Object,Object>> rdd = sc.textFile(input).map(x->{
//             String a[] = x.split(" ");
//             return new Tuple2<>(Long.valueOf(a[0]),Long.valueOf(a[1]));
//         });
//        Object a = 1;
//        Graph<Object, Object> graph = Graph.fromEdgeTuples(rdd.rdd(),a);
        //不要忘记去pom中增加相应的depedency
        //sc.sc() 代表这java的sc变成spark的sc的意思
        String a = "hehe";
        Graph<Object, Object> graph = GraphLoader.edgeListFile(sc.sc(),input,true,1,
                StorageLevel.MEMORY_AND_DISK_SER(), StorageLevel.MEMORY_AND_DISK_SER());

        //邊
        EdgeRDD<Object> edge = graph.edges();

        //點
        VertexRDD<Object> vertices = graph.vertices();

        //查看 邊的內容
        edge.toJavaRDD().foreach(new VoidFunction<Edge<Object>>() {
            public void call(Edge<Object> arg0) throws Exception {
                System.out.println(arg0.toString());
            }
        });

        //查看 點的內容
        vertices.toJavaRDD().foreach(new VoidFunction<Tuple2<Object,Object>>() {
            public void call(Tuple2<Object, Object> arg0) throws Exception {
                System.out.println(arg0.toString());
            }
        });


    }
}
