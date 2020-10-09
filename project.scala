// DATAFRAME PROJECT
// Use the Netflix_2011_2016.csv file to Answer and complete
// the commented tasks below!

// Start a simple Spark Session

// Load the Netflix Stock CSV File, have Spark infer the data types.
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header","true").option("inferSchema","true").csv("Netflix_2011_2016.csv")
// What are the column names?
df.printSchema()
for (col <- df.columns){
	println(col);
}
// What does the Schema look like?
df.describe().show()
// Print out the first 5 columns.
df.first(5).show()
// Use describe() to learn about the DataFrame.

// Create a new dataframe with a column called HV Ratio that
// is the ratio of the High Price versus volume of stock traded
// for a day.

// What day had the Peak High in Price?

// What is the mean of the Close column?

// What is the max and min of the Volume column?

// For Scala/Spark $ Syntax

// How many days was the Close lower than $ 600?

// What percentage of the time was the High greater than $500 ?

// What is the Pearson correlation between High and Volume?

// What is the max High per year?

// What is the average Close for each Calender Month?
