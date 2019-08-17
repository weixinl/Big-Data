package lwx.hw7
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark
import scala.io.Source
import java.io._
import org.apache.spark.sql.SparkSession
import scala.util.matching.Regex
import util.control.Breaks._
import scala.util._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.feature.Word2Vec  
import org.apache.spark.ml.feature.FeatureHasher
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.Row
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object Hello 
{

  def correlation():Unit=
  {
      val spark = SparkSession.builder().appName("example").master("local").getOrCreate()
      import spark.implicits._
      
      val training = spark.createDataFrame(Seq(
        (0L, "a b c d e spark", 1.0),
        (1L, "b d", 0.0),
        (2L, "spark f g h", 1.0),
        (3L, "hadoop mapreduce", 0.0),
        (4L, "b spark who", 1.0),
        (5L, "g d a y", 0.0),
        (6L, "spark fly", 1.0),
        (7L, "was mapreduce", 0.0),
        (8L, "e spark program", 1.0),
        (9L, "a e c l", 0.0),
        (10L, "spark compile", 1.0),
        (11L, "hadoop software", 0.0)
      )).toDF("id", "text", "label")
      
      
      
      
      // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
      val tokenizer = new Tokenizer()
        .setInputCol("text")
        .setOutputCol("words")
      val hashingTF = new HashingTF()
        .setInputCol(tokenizer.getOutputCol)
        .setOutputCol("features")
      val lr = new LogisticRegression()
        .setMaxIter(10)
      val pipeline = new Pipeline()
        .setStages(Array(tokenizer, hashingTF, lr))
       
//      var wordDF=tokenizer.transform(training)
//      var featurized=hashingTF.transform(wordDF)
//      featurized.show(false)
//      for(line<-featurized)
//      {
//        println(line)
//      }
//      return
      
      // We use a ParamGridBuilder to construct a grid of parameters to search over.
      // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
      // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
      val paramGrid = new ParamGridBuilder()
        .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
        .addGrid(lr.regParam, Array(0.1, 0.01))
        .build()
      
      // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
      // This will allow us to jointly choose parameters for all Pipeline stages.
      // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
      // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
      // is areaUnderROC.
      val cv = new CrossValidator()
        .setEstimator(pipeline)
        .setEvaluator(new BinaryClassificationEvaluator)
        .setEstimatorParamMaps(paramGrid)
        .setNumFolds(2)  // Use 3+ in practice
        .setParallelism(2)  // Evaluate up to 2 parameter settings in parallel
      
      // Run cross-validation, and choose the best set of parameters.
      val cvModel = cv.fit(training)
      
      // Prepare test documents, which are unlabeled (id, text) tuples.
      val test = spark.createDataFrame(Seq(
        (4L, "spark i j k"),
        (5L, "l m n"),
        (6L, "mapreduce spark"),
        (7L, "apache hadoop")
      )).toDF("id", "text")
      
      // Make predictions on test documents. cvModel uses the best model found (lrModel).
      cvModel.transform(test)
        .select("id", "text", "probability", "prediction")
        .collect()
        .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
          println(s"($id, $text) --> prob=$prob, prediction=$prediction")
        }
      
      


  }
  
  def sample_classify()
  {
    var libsvm_path="libsvm_higgs.txt"
//    var libsvm_path="sample_multiclass_classification_data.txt"
    var num_features=21
     val spark = SparkSession.builder().appName("example").master("local").getOrCreate()
     import spark.implicits._
    // Load the data stored in LIBSVM format as a DataFrame.
    val data = spark.read.format("libsvm").option("numFeatures",num_features)
      .load(libsvm_path)
//    data.show()
//      
//      return
    
//    for(line<-data.collect())
//    {
//      println(line)
//    }
//    data.show()
//    return
      
    // Split the data into train and test
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 1234L)
    val train = splits(0)
    val test = splits(1)
//    train.show()
//    test.show()
//    break
    
    // specify layers for the neural network:
    // input layer of size 4 (features), two intermediate of size 5 and 4
    // and output of size 3 (classes)
    val layers = Array[Int](21, 5, 4, 3)
    
    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(12)
      .setSeed(123)
      .setMaxIter(100)
    
    // train the model
    val model = trainer.fit(data)
    
    // compute accuracy on the test set
    val result = model.transform(data)
    
//    result.show()
//    val predictionAndLabels = result.select("prediction", "label").show()
    val predictionAndLabels = result.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")
    
    println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")

  }
  
  def main(args: Array[String]) 
  {
//      correlation();
    sample_classify();
   }
}