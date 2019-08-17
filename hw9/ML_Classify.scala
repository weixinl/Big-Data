package lwx.hw7
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import java.io._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.tuning.TrainValidationSplitModel
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import util.control.Breaks._

object ML_Classify 
{
  def gen_libsvm_file(csv_path:String,libsvm_path:String,feature_num:Int): Unit=
  {
    val csv_file=Source.fromFile(csv_path)
    val libsvm_writer = new PrintWriter(new File(libsvm_path))
    for(line <- csv_file.getLines)
    {
      var line_items=line.split(",")
      // one label, 28 features, use 21 features
      var label=line_items(0).toDouble.toInt
      libsvm_writer.write(label.toString);
      var use_feature_num=feature_num;
      for(feature_id <- 1 to use_feature_num)
      {
        breakable
        {
          var tmp_feature_value=line_items(feature_id).trim
          var feature_value=tmp_feature_value.toDouble
//          if(feature_value==0)
//            feature_value=0.00000001
          libsvm_writer.write(" ")
          libsvm_writer.write(feature_id.toString+":")
          libsvm_writer.write(feature_value.toString)
        }
      }
      libsvm_writer.write("\n")
    }
    csv_file.close
    libsvm_writer.close
  }
  
  def classify_test(libsvm_path:String):Unit=
  {
      val spark = SparkSession.builder().appName("ml").master("local").getOrCreate()
      val dataDF = spark.read.format("libsvm").load(libsvm_path)
      
//      for(line<-dataDF)
//      {
//        println(line)
//      }
//      dataDF.show()
//      return
      
      val Array(trainDF, testDF) = dataDF.randomSplit(Array(0.9, 0.1), seed = 12345)
      val layers = Array[Int](4, 5, 4, 3)
      val trainer = new MultilayerPerceptronClassifier()
        .setLayers(layers)
        .setBlockSize(128)
        .setSeed(1234L)
        .setMaxIter(100)
      val model = trainer.fit(trainDF)
      val result = model.transform(testDF)
      result.show()
      
//      var resDF=result.select("label", "prediction")
//      var test_num=0
//      var matched_num=0
//      for(line<-resDF.collect())
//      {
//        test_num+=1
//        var label=line(0).toString.toDouble.toInt
//        var prediction=line(1).toString.toDouble.toInt
//        if(label==prediction)
//          matched_num+=1
//      }

//      printf("accuracy: %d of %d\n",matched_num,test_num)
//            resDF.show()
      
  }
  
  def lr_classify(libsvm_path:String,model_path:String,feature_num:Int):Unit=
  {
    val spark = SparkSession.builder().appName("ml").master("local").getOrCreate()
    val dataDF = spark.read.format("libsvm").option("numFeatures",feature_num).load(libsvm_path)
//    dataDF.show()
//    return
   
    val Array(trainDF, testDF) = dataDF.randomSplit(Array(0.9, 0.1), seed = 123)
    
    val lr = new LogisticRegression().setMaxIter(10)
    val pipeline = new Pipeline().setStages(Array(lr))
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(1,0.5,0.3,0.1, 0.05,0.01,0.001))
      .build()

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      // 80% of the data will be used for training and the remaining 20% for validation.
      .setTrainRatio(0.8)
      // Evaluate up to 2 parameter settings in parallel
      .setParallelism(10)
    val model = trainValidationSplit.fit(trainDF)
    model.write.overwrite().save(model_path)
    var resDF=model.transform(testDF)
    resDF.show()
    resDF=resDF.select("label", "prediction")
    var test_num=0
    var matched_num=0
    for(line<-resDF.collect())
    {
      test_num+=1
      var label=line(0).toString.toDouble.toInt
      var prediction=line(1).toString.toDouble.toInt
      if(label==prediction)
        matched_num+=1
    }
    var acc=matched_num.toDouble/test_num
    printf("accuracy: %f %d of %d\n",acc,matched_num,test_num)   
  }
  
  def use_saved_model(libsvm_path:String,model_path:String,feature_num:Int):Unit=
  {
    val spark = SparkSession.builder().appName("ml").master("local").getOrCreate()
    val dataDF = spark.read.format("libsvm").option("numFeatures",feature_num).load(libsvm_path)
//    dataDF.show()
    val Array(trainDF, testDF) = dataDF.randomSplit(Array(0.9, 0.1), seed = 123)
    val my_model = TrainValidationSplitModel.load(model_path)
    var resDF=my_model.transform(testDF)
    resDF=resDF.select("label", "prediction")
    resDF.show()
    var test_num=0
    var matched_num=0
    for(line<-resDF.collect())
    {
      test_num+=1
      var label=line(0).toString.toDouble.toInt
      var prediction=line(1).toString.toDouble.toInt
      if(label==prediction)
        matched_num+=1
    }
    var acc=matched_num.toDouble/test_num
    printf("accuracy: %f %d of %d\n",acc,matched_num,test_num)
  }
  
  def perceptron_classify(libsvm_path:String,model_path:String,feature_num:Int):Unit=
  {

//    var libsvm_path="sample_multiclass_classification_data.txt"

     val spark = SparkSession.builder().appName("example").master("local").getOrCreate()
     import spark.implicits._
    // Load the data stored in LIBSVM format as a DataFrame.
    val data = spark.read.format("libsvm").option("numFeatures",feature_num)
      .load(libsvm_path)

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
    val layers = Array[Int](feature_num, 5, 4, 3)
    
    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(12)
      .setSeed(123)
      .setMaxIter(50)
    
    // train the model
    val model = trainer.fit(train)
    model.write.overwrite().save(model_path)
    
    // compute accuracy on the test set
    val result = model.transform(test)
    
//    result.show()
//    val predictionAndLabels = result.select("prediction", "label").show()
    val predictionAndLabels = result.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")
    println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")

  }
  
  def main(args: Array[String]) 
  {
//      var csv_path="HIGGS_simplified.csv"
//      var libsvm_path="libsvm_higgs_simplified.txt"
    var csv_path="higgs.csv"
    var libsvm_path="libsvm_higgs.txt"
    // feature_num:21 or 28
    var feature_num=28
    var lr_model_path="models/logistic_regression_model"
    var perceptron_model_path="models/multi_layer_perceptron_model"
//    gen_libsvm_file(csv_path,libsvm_path,feature_num)
    lr_classify(libsvm_path,lr_model_path,feature_num)
//    perceptron_classify(libsvm_path,perceptron_model_path,feature_num)
//    use_saved_model(libsvm_path,lr_model_path,feature_num)
//    use_saved_model(libsvm_path,perceptron_model_path,feature_num)
//    classify_test(libsvm_path);
  }
}