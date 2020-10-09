import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header","true").option("inferSchema","true").csv("CitiGroup2006_2008")

import spark.implicits._

df.filter($"Close">480).show()
// df.filter($"Close>480").show() equivalent as above
df.filter($"Close"<470 && $"High"<470).show()
// df.filter($"Close < 480 AND High <470").show()

val CH_low = df.filter("Close < 480 AND High < 480")
//.count to above returns 397
//.collect Collect (Action) - Return all the elements of the dataset as an array at 
// the driver program. This is usually useful after a filter or other operation that 
// returns a sufficiently small subset of the data.

//df.filter($"High"===484.40).show()  -> note == gives error here

df.select(corr("High","Low")).show() //0.9992999127..
