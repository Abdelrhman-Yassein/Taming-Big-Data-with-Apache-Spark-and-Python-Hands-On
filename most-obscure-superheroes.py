from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostObscureSuperheroes").getOrCreate()

schema = StructType([StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True)])

names = spark.read.schema(schema)\
    .option("sep", " ").csv("data/Marvel-names.txt")


lines = spark.read.text("data/Marvel-graph.txt")

connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0])\
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1)\
    .groupBy("id").agg(func.sum("connections").alias("connections"))



minConnectionCount = connections.agg(func.min("connections")).first()[0]

minConnections = connections.filter(
    func.col("connections") == minConnectionCount)

minConnectionWithNames = minConnections.join(names, "id")
print("The following characters have only " +
      str(minConnectionCount) + " connection(s):")

minConnectionWithNames.select("name").show()
