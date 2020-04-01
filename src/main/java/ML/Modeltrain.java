package ML;

import org.apache.spark.SparkConf;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.List;

public class Modeltrain {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("model training").setMaster("local[*]");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        String modelpath = "";
        String inputdata = "/home/linjiaqin/下载/data_onehot1.csv";
        String test = "/home/linjiaqin/下载/off_data_onehot1.csv";
        Dataset<Row> data = sparkSession.read()
                .option("header", true)
                .option("inferSchema", false) //自动推断，否则默认为String类型
                .csv(inputdata);

        Dataset<Row> testdata = sparkSession.read()
                .option("header", true)
                .option("inferSchema", false) //自动推断，否则默认为String类型
                .csv(test);
        System.out.println("原始数据");
        data.show();
        data.printSchema();
        //这里看到输出的每一列都是string，不行，得转成double
        String[] columnName = data.columns();
        //String schemaString = "age sex cp trestbps chol fbs restecg thalach exang oldpeak slope ca thal target";

        //定义注册udf函数
        sparkSession.udf().register("toDouble", new UDF1<String, Double>() {
            public Double call(String s) throws Exception {
                return Double.valueOf(s);
            }
        }, DataTypes.DoubleType);
        //将每一列类型强制转化为double
        for(int i = 1; i < columnName.length; i++){
            data = data.withColumn(columnName[i], functions.callUDF("toDouble", functions.col(columnName[i])));
            testdata = testdata.withColumn(columnName[i], functions.callUDF("toDouble", functions.col(columnName[i])));
        }

        System.out.println("dataframe 数据");
        data.show();
        data.printSchema();



        //因为sparkml支持的数据格式是<Vector，label>，所以要把当个数据集成成一个Vector
        String[] vecCol = new String[columnName.length-3];
        int j = 0;
        for (int i = 1; i < columnName.length-1; i++) {
            if (!columnName[i].equals("target")) vecCol[j++] = columnName[i];
        }
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(vecCol)
                .setOutputCol("features");

        //因为label不能是string类型？同时训练的目标列名必须是label，特征列名必须是features
        StringIndexer labelIndexer = new StringIndexer()
                .setInputCol("target")
                .setOutputCol("label");

        //设置逻辑回归作为训练模型
        //LogisticRegression is an Estimator, and calling fit() trains a LogisticRegressionModel, which is a Model and hence a Transformer.
        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.001);

        IndexToString indexToString = new IndexToString()
                .setInputCol("label")
                .setOutputCol("targets");            //不能喝已有的列同名

        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] {assembler, labelIndexer, lr, indexToString});

        PipelineModel model = pipeline.fit(data);


        Dataset<Row> predictions = model.transform(testdata);
        predictions.show();
        //获取分类结果的测评器
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setMetricName("accuracy");

        // compute the classification error on test data.
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Test Error = " + (1 - accuracy));
        List list = predictions.select("features", "label", "prediction", "targets")
                .collectAsList();
        for (Object e:list) {
            System.out.println(e.toString());
        }




//        //保存模型
//        try {
//            model.save(modelpath);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        // Print the coefficients and intercept for multinomial logistic regression
//        System.out.println("Coefficients: \n"
//                + model.coefficientMatrix() + " \nIntercept: " + model.interceptVector());
//        LogisticRegressionTrainingSummary trainingSummary = model
//
//// Obtain the loss per iteration.
//        double[] objectiveHistory = trainingSummary.objectiveHistory();
//        for (double lossPerIteration : objectiveHistory) {
//            System.out.println(lossPerIteration);
//        }
//
//// for multiclass, we can inspect metrics on a per-label basis
//        System.out.println("False positive rate by label:");
//        int i = 0;
//        double[] fprLabel = trainingSummary.falsePositiveRateByLabel();
//        for (double fpr : fprLabel) {
//            System.out.println("label " + i + ": " + fpr);
//            i++;
//        }
//
//        System.out.println("True positive rate by label:");
//        i = 0;
//        double[] tprLabel = trainingSummary.truePositiveRateByLabel();
//        for (double tpr : tprLabel) {
//            System.out.println("label " + i + ": " + tpr);
//            i++;
//        }
//
//        System.out.println("Precision by label:");
//        i = 0;
//        double[] precLabel = trainingSummary.precisionByLabel();
//        for (double prec : precLabel) {
//            System.out.println("label " + i + ": " + prec);
//            i++;
//        }
//
//        System.out.println("Recall by label:");
//        i = 0;
//        double[] recLabel = trainingSummary.recallByLabel();
//        for (double rec : recLabel) {
//            System.out.println("label " + i + ": " + rec);
//            i++;
//        }
//
//        System.out.println("F-measure by label:");
//        i = 0;
//        double[] fLabel = trainingSummary.fMeasureByLabel();
//        for (double f : fLabel) {
//            System.out.println("label " + i + ": " + f);
//            i++;
//        }
//
//        double accuracy = trainingSummary.accuracy();
//        double falsePositiveRate = trainingSummary.weightedFalsePositiveRate();
//        double truePositiveRate = trainingSummary.weightedTruePositiveRate();
//        double fMeasure = trainingSummary.weightedFMeasure();
//        double precision = trainingSummary.weightedPrecision();
//        double recall = trainingSummary.weightedRecall();
//        System.out.println("Accuracy: " + accuracy);
//        System.out.println("FPR: " + falsePositiveRate);
//        System.out.println("TPR: " + truePositiveRate);
//        System.out.println("F-measure: " + fMeasure);
//        System.out.println("Precision: " + precision);
//        System.out.println("Recall: " + recall);
    }
}
