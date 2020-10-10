import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header","true").option("inferSchema","true").csv("Sales.csv")

import spark.implicits._

// df.groupBy("Company").mean().show()  works for min(), sum() as well
//|Company|       avg(Sales)|
//+-------+-----------------+
//|   GOOG|            220.0|
//|     FB|            610.0|
//|   MSFT|322.3333333333333|
//+-------+-----------------+


// Other Aggregate Functions
// http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$
//df.select(countDistinct("Sales")).show() //approxCountDistinct
//df.select(sumDistinct("Sales")).show()
//df.select(variance("Sales")).show()
//df.select(stddev("Sales")).show() //avg,max,min,sum,stddev
//df.select(collect_set("Sales")).show() //returns a set of objects with duplicate elements eliminated.

// OrderBy
// Ascending
df.orderBy("Sales").show()

// Descending
df.orderBy($"Sales".desc).show()