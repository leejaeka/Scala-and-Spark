import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header","true").option("inferSchema","true").csv("CitiGroup2006_2008")

import spark.implicits._

//df.select(year(df("Date"))).show()
//df.select(month(df("Date"))).show()

val df2 = df.withColumn("Year", year(df("Date")))
val dfavgs = df2.groupBy("Year").mean()
dfavgs.select($"Year",$"avg(Close)").show()
//+----+------------------+
//|Year|        avg(Close)|
//+----+------------------+
//|2007| 477.8203984063745|
//|2006| 489.2697211155379|
//|2008|190.48893280632404|
//+----+------------------+