import org.apache.spark.sql.SparkSession

val spark = SparkSession.build().getOrCreate()
val df = spark.read.csv("CitiGroup2006_2008")
val df2 = spark.read.option("header","true").option("inferSchema","true").csv("CitiGroup2006_2008")

df.head(5)

for (line<- df2.head(5)){
	println(line)
}
//df2.describe().show()
/*+-------+----------+------------------+-----------------+------------------+------------------+-----------------+
|summary|      Date|              Open|             High|               Low|             Close|           Volume|
+-------+----------+------------------+-----------------+------------------+------------------+-----------------+
|  count|       755|               755|              755|               755|               755|              755|
|   mean|      null| 386.0923178807949|390.6590596026489|380.80170860927143| 385.3421456953643|6308596.382781457|
| stddev|      null|149.32301134820133|148.5151130063523|150.53136890891344|149.83310074439177| 8099892.56297633|
|    min|2006-01-03|              54.4|             55.3|              30.5|              37.7|           632860|
|    max|2008-12-31|             566.0|            570.0|             555.5|             564.1|        102869289|
+-------+----------+------------------+-----------------+------------------+------------------+-----------------+ */

//df2.select("volume")
//df2.select($"Date",$"Close") to select multiple
//val df3 = df2.withColumn("HighPlusLow", df("High")+df("Low")) Add new column
// df3.select(df3("HighPlusLow").as("HPL"), df3("Close")).show()   renaming