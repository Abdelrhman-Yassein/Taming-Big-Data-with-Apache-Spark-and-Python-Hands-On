from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StringType, IntegerType, StructField, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema = StructType([StructField("stationID", StringType(
), nullable=True), StructField("date", IntegerType(), nullable=True),
    StructField("measure_type", StringType(), nullable=True),
    StructField("temperature", FloatType(), nullable=True)])

# Read the file as a datafram
df = spark.read.schema(schema).csv("data/1800.csv")
df.printSchema()

# Filter out all but TMIN entries
minTemps = df.filter(df.measure_type == "TMIN")

# Select only stationID and temperature
sectionTemp = df.select("stationID", "temperature")

# Aggregate to find minimum temperature for every station
minTempsByStation = sectionTemp.groupBy("stationID").min("temperature")


# Convert temperature to fahrenheit and sort the dataset
minTempsByStationF = minTempsByStation.withColumn("temperature",
                                                  func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2))\
    .select("stationID", "temperature").sort("temperature")

# Collect, format, and print the results
results = minTempsByStationF.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))

spark.stop()
