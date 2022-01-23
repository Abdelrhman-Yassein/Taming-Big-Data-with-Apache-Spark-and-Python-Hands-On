from dataclasses import field
from pyexpat import model
from unicodedata import name
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, LongType
from pyspark.ml.recommendation import ALS
import sys
import codecs


def loadMovieNames():
    movieNames = {}
    with codecs.open("data/ml-100k/u.item", 'r', encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]

    return movieNames


spark = SparkSession.builder.appName("ALSExample").getOrCreate()

movieSchema = StructType([StructField("userID", IntegerType(), True),
                          StructField("movieID", IntegerType(), True),
                          StructField("rating", IntegerType(), True),
                          StructField("timestamp", LongType(), True)])

names = loadMovieNames()

ratings = spark.read.option("sep", "\t")\
    .schema(movieSchema).csv("data/ml-100k/u.data")

als = ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userID").setItemCol("movieID")\
    .setRatingCol("rating")

model = als.fit(ratings)
# Manually construct a dataframe of the user ID's we want recs for
userID = int(sys.argv[1])
userSchema = StructType([StructField("userID", IntegerType(), True)])
users = spark.createDataFrame([[userID, ]], userSchema)

recommendations = model.recommendForUserSubset(users, 10).collect()


print("Top 10 recommendations for user ID " + str(userID))

for userRecs in recommendations:
    # userRecs is (userID, [Row(movieId, rating), Row(movieID, rating)...])
    myRecs = userRecs[1]
    for rec in myRecs:  # my Recs is just the column of recs for the user
        # For each rec in the list, extract the movie ID and rating
        movie = rec[0]
        rating = rec[1]
        movieName = names[movie]
        print(movieName + str(rating))
