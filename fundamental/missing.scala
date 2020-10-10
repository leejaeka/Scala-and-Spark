import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

val df = spark.read.option("header","true").option("inferSchema", "true").csv("ContainsNull.csv")

df.printSchema()
df.show()

//df.na.drop().show() 
//---+-----+-----+
//|  Id| Name|Sales|
//+----+-----+-----+
//|emp4|Cindy|456.0|
//+----+-----+-----+

//df.na.drop(2).show()    drop any row less than 2 non-null values
//+----+-----+-----+
//|  Id| Name|Sales|
//+----+-----+-----+
//|emp1| John| null|
//|emp3| null|345.0|
//|emp4|Cindy|456.0|
//+----+-----+-----+

//df.na.fill(100).show() matches data type then replace
//+----+-----+-----+
//|  Id| Name|Sales|
//+----+-----+-----+
//|emp1| John|100.0|
//|emp2| null|100.0|
//|emp3| null|345.0|
//|emp4|Cindy|456.0|
//+----+-----+-----+

//df.na.fill("New Name", Array("Name")).show()  one column