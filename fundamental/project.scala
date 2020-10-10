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
// Use describe() to learn about the DataFrame.

// Create a new dataframe with a column called HV Ratio that
// is the ratio of the High Price versus volume of stock traded
// for a day.
//val df2 = df.withColumn("HV Ratio", df("High")/df("Volume"))
// What day had the Peak High in Price?
//df.select(max("High")).show()
// What is the mean of the Close column?
//df.select(mean("Close")).show()
// What is the max and min of the Volume column?
//df.select(max("Volume"),min("Volume")).show()
// For Scala/Spark $ Syntax

// How many days was the Close lower than $ 600?
df.filter($"Close"< 600).count()
// What percentage of the time was the High greater than $500 ?
df.groupBy($"High">500).count()/len(df.select("High))
// What is the Pearson correlation between High and Volume?
//df.select(corr("High","Low")).show()
// What is the max High per year?
val df2 = df.withColumn("Year", year(df("Date")))
val dfavgs = df2.groupBy("Year").max()
dfavgs.select($"Year",$"max(High)").show()
// What is the average Close for each Calender Month?
val df3 = df.withColumn("Month", month(df("Date")))
val dfavgs = df3.groupBy("Month").mean()
dfavgs.select($"Month",$"avg(Close)").orderBy("Month").show()