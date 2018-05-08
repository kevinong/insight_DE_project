import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, functions
from pyspark.sql.types import FloatType, ArrayType, StringType, IntegerType

import os
from textblob import TextBlob
import datetime

from util import get_s3_path
#import products_data


BUCKET = "amazon-data-insight"

def sentiment(reviewText):
    sentiment = TextBlob(reviewText).sentiment
    return (sentiment.polarity, sentiment.subjectivity)

def flat(input_list):
    result = []
    if input_list is None:
        return result
    for sublist in input_list:
        if sublist is not None:
            for val in sublist:
                if val is not None and val not in result:
                    result.append(val)
    return result

class ProductData:
    def __init__(self, path, conf, sc):
        sqlContext = SQLContext(sc)
        self.df = sqlContext.read.format('json').\
            options(header='true', inferSchema='true').\
            load(path)

    def main(self):
        # self.df.select("categories").show(10, False)

        flat_udf = functions.udf(flat, ArrayType(StringType()))

        self.df = self.df.withColumn("categories", flat_udf(self.df.categories))
        # self.df.select("categories").show(10, False)

        self.df = self.df.withColumn("related", flat_udf(self.df.related))
        # self.df.select("related").show(10, False)


class ReviewsData:
    def __init__(self, path, conf, sc):
        sqlContext = SQLContext(sc)
        self.df = sqlContext.read.format('json').\
            options(header='true', inferSchema='true').\
            load(path)

    def joinDF(self, prod_df):
        prod_df = prod_df.withColumnRenamed('asin', 'asin2')
        self.df = self.df.join(prod_df, self.df.asin == prod_df.asin2)

    def main(self):

        # self.df.show(5)

        print "start time: ", datetime.datetime.now()

        polarity_udf = functions.udf(lambda reviewText: TextBlob(reviewText).sentiment.polarity, FloatType())
        subjectivity_udf = functions.udf(lambda reviewText: TextBlob(reviewText).sentiment.subjectivity, FloatType())

        pos_polarity_udf = functions.udf(lambda pol: pol if pol > 0 else 0.0, FloatType())
        neg_polarity_udf = functions.udf(lambda pol: pol if pol < 0 else 0.0, FloatType())
        
        pos_count_udf = functions.udf(lambda pol: 1 if pol > 0 else 0, IntegerType())
        neg_count_udf = functions.udf(lambda pol: 1 if pol < 0 else 0, IntegerType())

        print "start transform 1: ", datetime.datetime.now()
        # Transforming review data
        self.df = self.df\
                            .withColumn("polarity", polarity_udf(self.df.reviewText))\
                            .withColumn("subjectivity", subjectivity_udf(self.df.reviewText))\
                            .withColumn("helpful_vote", self.df.helpful[0])\
                            .withColumn("unhelpful_vote", self.df.helpful[1])

        # self.df.show(5)
        print "start transfrom 2: ", datetime.datetime.now()

        self.df = self.df\
                            .withColumn("pos_polarity", pos_polarity_udf(self.df.polarity))\
                            .withColumn("neg_polarity", neg_polarity_udf(self.df.polarity))\
                            .withColumnRenamed("reviewerID", "reviewerid")\
                            .withColumn("pos_review_count", pos_count_udf(self.df.polarity))\
                            .withColumn("neg_review_count", neg_count_udf(self.df.polarity))
#                            .withColumnRenamed("categories", "cat")



        print 'transformation done: ', datetime.datetime.now()
        # self.df.show(5)

        # print 'join', datetime.datetime.now()
        # self.joinDF(prod_df)
        # # self.df.show(5)

        # print 'join done'

        # grouped_df = self.df.groupby("reviewerid").agg(functions.avg("overall").alias("avg_star"), \
        #                                                        functions.sum("helpful_vote").alias("helpful"), \
        #                                                        functions.sum("unhelpful_vote").alias("unhelpful"), \
        #                                                        functions.avg("polarity").alias("avg_pol"), \
        #                                                        functions.sum("pos_polarity").alias("pos"), \
        #                                                        functions.sum("pos_review_count").alias("pos_review_count"),\
        #                                                        functions.sum("neg_polarity").alias("neg"),\
        #                                                        functions.sum("neg_review_count").alias("neg_review_count"),\
        #                                                        functions.avg("subjectivity").alias("subjectivity"),\
        #                                                        functions.collect_set("asin").alias("products"),\
        #                                                        functions.collect_set("categories").alias("categories"))

        # print 'group done'
#        grouped_df.write.format("txt").save("df.txt")
#        grouped_df.rdd.saveAsTextFile("df.txt")
 #       grouped_df.show(20)

        # grouped_df.write.format("org.apache.spark.sql.cassandra").options(table = "data", keyspace = "amazonreviews").save()

        # table1 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="kv", keyspace="ks").load()
        # table1.write.format("org.apache.spark.sql.cassandra").options(table="othertable", keyspace = "ks").save(mode ="append")

def joinDF2(rev_df, prod_df):
    prod_df = prod_df.withColumnRenamed('asin', 'asin2')
    joined_df = grouped_df.join(prod_df, rev_df.asin == prod_df.asin2)

    joined_df = rev_df.groupby("reviewerid").agg(functions.avg("overall").alias("avg_star"), \
                                                   functions.sum("helpful_vote").alias("helpful"), \
                                                   functions.sum("unhelpful_vote").alias("unhelpful"), \
                                                   functions.avg("polarity").alias("avg_pol"), \
                                                   functions.sum("pos_polarity").alias("pos"), \
                                                   functions.sum("pos_review_count").alias("pos_review_count"),\
                                                   functions.sum("neg_polarity").alias("neg"),\
                                                   functions.sum("neg_review_count").alias("neg_review_count"),\
                                                   functions.avg("subjectivity").alias("subjectivity"),\
                                                   functions.collect_set("asin").alias("products"),\
                                                   functions.collect_set("categories").alias("categories"))

    return joined_df

if __name__ == "__main__":

    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')
    # fs = s3fs.S3FileSystem(anon = True)
    # print(fs.ls('s3a://{}/{}/'.format(BUCKET, 'qa')))


    conf = SparkConf().setAppName("test").set("spark.driver.maxResultSize", "2g").set("spark.driver.memory", "3g")
    sc = SparkContext(conf = conf)
    # sc.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", aws_access_key)
    # sc.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", aws_secret_access_key)
    sc._jsc.hadoopConfiguration().set('fs.s3n.awsAccessKeyId', aws_access_key)
    sc._jsc.hadoopConfiguration().set('fs.s3a.awsSecretAccessKey',aws_secret_access_key)

    # reviews_path = get_s3_path(BUCKET, 'reviews', 'reviews_Clothing_Shoes_and_Jewelry_5.json')
    # reviews_path = get_s3_path(BUCKET, 'reviews', 'complete.json')
    # products_path = get_s3_path(BUCKET, "product", "metadata.json")

    reviews_path = get_s3_path(BUCKET, "reviews", "reviews_Toys_and_Games.json")
    products_path = get_s3_path(BUCKET, "product", "meta_Toys_and_Games.json")

    productsData = ProductData(products_path, conf, sc)
    productsData.main()

    prod_df = productsData.df.select("asin", "categories", "related")
    # print 'show product data'
    # prod_df.show(10)

    reviewsData = ReviewsData(reviews_path, conf, sc)
    reviewsData.main()

    joined_df = joinDF2(reviewsData.df, prod_df)
    joined_df.write.format("org.apache.spark.sql.cassandra").options(table = "data", keyspace = "amazonreviews").save()





