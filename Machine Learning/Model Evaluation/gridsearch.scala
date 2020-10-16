import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

val data = spark.read.option("header","true").option("inferSchema", "true").format("csv").load("Clean_USA_HOUSING.csv")
data.head(2)

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

val df = data.select(data("Price").as("label"),$"Avg Area Income", $"Avg Area House Age", $"Avg Area Number of Rooms", $"Avg Area Number of Bedrooms", $"Area Population")

val assembler = new VectorAssembler().setInputCols(Array("Avg Area Income", "Avg Area House Age", "Avg Area Number of Rooms", "Avg Area Number of Bedrooms", "Area Population")).setOutputCol("features")

val output = assembler.transform(df).select($"label",$"features")

// training and test data
val Array(training, test) = output.select("label", "features").randomSplit(Array(0.7,0.3),seed=41)

// model
val lr = new LinearRegression()

// GridSearchCV
val paramGrid = new ParamGridBuilder().build()

// Train Split (holdout)
val trainvalsplit = (new TrainValidationSplit().setEstimator(lr).setEvaluator(new RegressionEvaluator())
					.setEstimatorParamMaps(paramGrid).setTrainRatio(0.8))

val model = trainvalsplit.fit(training)

model.transform(test).select("features","label","prediction").show()

