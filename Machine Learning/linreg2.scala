import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

// To see less warnings
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)


// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// Prepare training and test data.
val data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("Clean_USA_Housing.csv")

// Check out the Data
data.printSchema()

// recall ("label", "features")
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

val df = data.select(data("Price").as("label"), 
	$"Avg Area Income", $"Avg Area House Age", $"Avg Area Number of Rooms", 
		$"Avg Area Number of Bedrooms", $"Area Population")

val assembler = (new VectorAssembler().setInputCols(Array("Avg Area House Age", "Avg Area Number of Rooms", 
		"Avg Area Number of Bedrooms", "Area Population")).setOutputCol("features"))

val output = assembler.transform(df).select($"label",$"features")
val lr = new LinearRegression()
val lrModel = lr.fit(output)

val trainingSum = lrModel.summary
trainingSum.residuals.show()