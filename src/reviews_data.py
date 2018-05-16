import re

import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, functions
from pyspark.sql.types import FloatType, ArrayType, StringType, IntegerType
from textblob import TextBlob

from util import get_s3_path


BUCKET = "amazon-data-insight"


def sentiment(reviewText):
    sentiment = TextBlob(reviewText).sentiment
    return (sentiment.polarity, sentiment.subjectivity)



class ReviewsData:
    def __init__(self, category, conf, sc):
        self.category = category
        path = get_s3_path(BUCKET, "reviews", "reviews_{}.json".format(CATEGORIES[category]))
        sqlContext = SQLContext(sc)
        self.df = sqlContext.read.format('json').\
            options(header='true', inferSchema='true').\
            load(path)

    def transform(self):
        polarity_udf = functions.udf(lambda reviewText: TextBlob(reviewText).sentiment.polarity, FloatType())
        subjectivity_udf = functions.udf(lambda reviewText: TextBlob(reviewText).sentiment.subjectivity, FloatType())

        pos_polarity_udf = functions.udf(lambda pol: pol if pol > 0 else 0.0, FloatType())
        neg_polarity_udf = functions.udf(lambda pol: pol if pol < 0 else 0.0, FloatType())
        
        pos_count_udf = functions.udf(lambda pol: 1 if pol > 0 else 0, IntegerType())
        neg_count_udf = functions.udf(lambda pol: 1 if pol < 0 else 0, IntegerType())

        self.df = self.df\
                            .withColumn("helpful_vote", self.df.helpful[0])\
                            .withColumn("unhelpful_vote", self.df.helpful[1])\
                            .withColumn("polarity", polarity_udf(self.df.reviewText))\
                            .withColumn("subjectivity", subjectivity_udf(self.df.reviewText))\

        self.df = self.df\
                            .withColumn("pos_polarity", pos_polarity_udf(self.df.polarity))\
                            .withColumn("neg_polarity", neg_polarity_udf(self.df.polarity))\
                            .withColumnRenamed("reviewerID", "reviewerid")\
                            .withColumn("pos_review_count", pos_count_udf(self.df.polarity))\
                            .withColumn("neg_review_count", neg_count_udf(self.df.polarity))

        self.user_df = self.df.groupby("reviewerid").agg(functions.avg("overall").alias("avg_star"), \
                                                               functions.count("overall").alias('count'),\
                                                               functions.sum("helpful_vote").alias("helpful"), \
                                                               functions.sum("unhelpful_vote").alias("unhelpful"),\
                                                               functions.avg("polarity").alias("avg_pol"), \
                                                               functions.sum("pos_polarity").alias("pos"), \
                                                               functions.sum("pos_review_count").alias("pos_review_count"),\
                                                               functions.sum("neg_polarity").alias("neg"),\
                                                               functions.sum("neg_review_count").alias("neg_review_count"),\
                                                               functions.avg("subjectivity").alias("subjectivity"))

        self.user_df.write.jdbc(url=postgres_url, table='{}users'.format(self.category), mode='overwrite', properties=postgres_properties)









